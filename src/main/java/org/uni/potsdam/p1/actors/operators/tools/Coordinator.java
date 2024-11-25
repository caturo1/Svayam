package org.uni.potsdam.p1.actors.operators.tools;

import com.google.ortools.Loader;
import com.google.ortools.linearsolver.MPConstraint;
import com.google.ortools.linearsolver.MPObjective;
import com.google.ortools.linearsolver.MPSolver;
import com.google.ortools.linearsolver.MPVariable;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;

import java.util.*;

/**
 * <p>
 * This class coordinates the dynamic change of the load shedding shares of each operator.
 * If an analyser detects an overload it will send a job message to an instance of this
 * class (empty Metrics object with "overloaded" description). This message will be stored
 * in a jobQueue, from which the Coordinator will only take and solve one job at a time.
 * </p>
 * <p>
 * The coordinator keeps an array with the OperatorInfo of all operators in the operator
 * graph. Initially the instances in this array are all empty (no metrics), but as soon
 * as the coordinator accepts a job, it will contact every operator in the system through
 * kafka, so that they save and forward their current metrics ( this is the base logic
 * of the coordinator's snapshot procedure).
 * </p>
 * <p>
 * Once the coordinator has gathered all the metrics from all the operators it will then
 * commence the calculation of a load shedding configuration using a LP-Solver. The results
 * of the solver are then sent to the operator which requested help through kafka.
 * </p>
 */
public class Coordinator extends KeyedProcessFunction<Long, Metrics, String> {

  public HashMap<String, Integer> indexer;
  public OperatorInfo[] operatorsList;
  public ValueState<Metrics> lambda;
  public Set<String> jobQueue;
  public OutputTag<String> sosOutput;
  public Set<String> sinkOperators;

  // very low latency bound for debugging - must be tweaked with
  public double bound;

  public Coordinator() {
  }

  public Coordinator(OutputTag<String> sosOutput, double bound, OperatorInfo... operators) {
    indexer = new HashMap<>(operators.length);
    operatorsList = new OperatorInfo[operators.length];
    sinkOperators = new HashSet<>();
    for (int i = 0; i < operators.length; i++) {
      OperatorInfo current = operators[i];
      indexer.put(current.name, i);
      operatorsList[i] = current;
      if (current.isSinkOperator) {
        sinkOperators.add(current.name);
      }
    }
    jobQueue = new LinkedHashSet<>(operators.length);
    this.sosOutput = sosOutput;
    this.bound = bound;

  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    lambda = getRuntimeContext().getState(new ValueStateDescriptor<>("lambda", Metrics.class));
  }

  @Override
  public void processElement(Metrics value, KeyedProcessFunction<Long, Metrics, String>.Context ctx, Collector<String> out) throws Exception {

    // if sos-message: add operator's name to the jobQueue if it not already there
    if (value.description.equals("overloaded")) {
      if (jobQueue.isEmpty()) {
        operatorsList[indexer.get(value.name)].isOverloaded = true;
        ctx.output(sosOutput, "snap:" + System.nanoTime());
      }
      jobQueue.add(value.name);
    } else {

      // if metrics: add to the respective OperationInfo instance
      operatorsList[indexer.get(value.name)].put(value.description, value);

    }

    // check if all OperatorInfo instances are complete
    boolean isReady = true;
    for (OperatorInfo operator : operatorsList) {
      isReady &= operator.isReady();
    }

    //if yes, start solver
    if (isReady) {

      // define overloaded operator
      String lastOverloadedOperator = jobQueue.iterator().next();
      OperatorInfo overloadedOperatorInfo = operatorsList[indexer.get(lastOverloadedOperator)];

      // import libraries
      Loader.loadNativeLibraries();

      // declare solver
      MPSolver solver = MPSolver.createSolver("GLOP");

      List<MPVariable> sinksList = new ArrayList<>(sinkOperators.size());
      Map<String, MPVariable> patternsDvs = new HashMap<>(operatorsList.length * overloadedOperatorInfo.patterns.length);
      Deque<OperatorInfo> nodeQueue = new ArrayDeque<>(operatorsList.length);
      nodeQueue.offer(overloadedOperatorInfo);

      int numberOfInputs = overloadedOperatorInfo.inputTypes.length;
      int numberOfPatterns = overloadedOperatorInfo.patterns.length;
      List<MPVariable> xDvList = new ArrayList<>(numberOfInputs * numberOfPatterns);

      /*
       * Define the  time constraint -> calculate average processing time for the given
       * snapshot and measure it against the ideal value p* (calculated with the latency bound and totalInputRate at the overloaded operator)
       */
      double totalInputRate = overloadedOperatorInfo.getMetric("lambdaIn").get("total");
      double totalProcessingTime = overloadedOperatorInfo.getMetric("ptime").get("total");
      double p = Arrays.stream(overloadedOperatorInfo.inputTypes).map(inputType -> (overloadedOperatorInfo.getMetric("lambdaIn").get(inputType) / totalInputRate) * totalProcessingTime).reduce(0., Double::sum);
      double pStar = 1 / ((1 / bound) + totalInputRate);
      MPConstraint timeConstraint = solver.makeConstraint(p - pStar, MPSolver.infinity(), "time_constraint");
      double ptime = overloadedOperatorInfo.getMetric("ptime").get(overloadedOperatorInfo.patterns[0].name);

      /*
       * Create decision variables for the overloaded operator (creates x-dvs for every input type of every pattern in the range [0,1])
       * and add them to the time constraint.
       */
      String patternName = overloadedOperatorInfo.patterns[0].name;
      for (int i = 0, indexPatterns = 0; i < numberOfInputs * numberOfPatterns; i++) {
        String type = overloadedOperatorInfo.inputTypes[i % numberOfInputs];
        String variableName = patternName + "_" + type;
        MPVariable xDv = solver.makeNumVar(0., 1., variableName);
        patternsDvs.put(variableName, xDv);
        xDvList.add(xDv);
        double factor = (overloadedOperatorInfo.getMetric("lambdaIn").get(type) / totalInputRate) * ptime;
        timeConstraint.setCoefficient(xDv, factor);

        if ((i + 1) % numberOfInputs == 0 && indexPatterns < numberOfPatterns - 1) {
          ptime = overloadedOperatorInfo.getMetric("ptime").get(overloadedOperatorInfo.patterns[++indexPatterns].name);
          patternName = overloadedOperatorInfo.patterns[indexPatterns].name;
        }
      }

      double infinity = Double.POSITIVE_INFINITY;

      /*
       * Traverse tree of operators from the overloaded operator up to the leaf nodes
       * in a breadth first fashion. Generate decision variables  (y-dvs) for all operators'
       * patterns. Set constraints for every y-dv generated in every pattern and for each
       * input type of that pattern.
       * Store the y-dv appropriately in a map, so that their successors can access it.
       * We link each y-dv with every pattern in the following downstream operator.
       */
      while (!nodeQueue.isEmpty()) {

        // fetch current operator
        OperatorInfo currentNode = nodeQueue.poll();
        if (nodeQueue.contains(currentNode)) {
          continue;
        }
        boolean isOverloadedOp = currentNode == overloadedOperatorInfo;
        // iterate through patterns
        for (EventPattern currentPattern : currentNode.patterns) {

          // fetch weight maps for the corresponding pattern
          Map<String, Integer> weights = currentPattern.getWeightMaps();

          /*
           * Create decision variable y-dv for this pattern - it is bounded by the total
           * processing rates of this operator (output constraint)
           */
          MPVariable patternYDv = solver.makeNumVar(0, currentNode.getValue("mu", "total"), currentNode.name + "_" + currentPattern.name);
          boolean isOr = currentPattern.getType().equals("OR");

          // iterate through the input types of this operator
          for (String inputType : weights.keySet()) {

            /*
             * Set constraint bounds:
             *  1) Operator is overloaded:
             *      AND/SEQ/ID:
             *           y_out <= ( 1 - x_in) * lambdaIn * (1 / eventsInPattern)
             *        => y_out * eventsInPattern + x_in * lambdaIn <= lambdaIn
             *      OR:
             *           y_out >= ( 1 - x_in) * lambdaIn * (1 / eventsInPattern)
             *        => y_out * eventsInPattern + x_in * lambdaIn >= lambdaIn
             *
             *  2) Operator is not overloaded and there is no decision variable for this
             *     input type ( input type doesn't originate from an operator in the tree
             *     being traversed).
             *      AND/SEQ/ID:
             *           y_out <= lambdaIn
             *      OR:
             *           y_out >= lambdaIn
             *
             *  3) Operator is not overloaded and there is a decision variable for this
             *     input type ( input type originate from an operator in the tree being
             *     traversed).
             *      AND/SEQ/ID:
             *           y_out <= y_in * lambdaIn * (1 / eventsInPattern)
             *        => y_out * eventsInPattern - y_in * lambdaIn <= 0
             *      OR:
             *           y_out >= y_in * lambdaIn * (1 / eventsInPattern)
             *        => y_out * eventsInPattern - y_in * lambdaIn >= 0
             */
            double factor = currentNode.getValue("lambdaIn", inputType);
            double bound = factor;

            // check case 3
            String dvName = currentPattern.name + "_" + inputType;
            boolean dvExists = patternsDvs.containsKey(dvName);
            if (dvExists && !(isOverloadedOp)) {
              bound = 0;
              factor = -1;
            }

            // set selectivity constraints
            MPConstraint constraint;
            if (isOr) {
              constraint = solver.makeConstraint(bound, infinity, dvName);
            } else {
              constraint = solver.makeConstraint(-infinity, bound, dvName);
            }
            constraint.setCoefficient(patternYDv, weights.getOrDefault(inputType, 1));
            if (dvExists) {
              constraint.setCoefficient(patternsDvs.get(dvName), factor);
            }
          }

          /*
           * Include the y-dv just created in the map of decision variables, using the
           * pattern types of the operators downstream to differentiate the access keys
           * operator- and pattern-wise.
           */
          for (String downStreamOpName : currentPattern.downstreamOperators) {
            OperatorInfo downStreamOp = operatorsList[indexer.get(downStreamOpName)];
            for (EventPattern downStreamPattern : downStreamOp.patterns) {
              String downStreamPatternName = downStreamPattern.name;
              String variableName = downStreamPatternName + "_" + currentPattern.name;
              patternsDvs.put(variableName, patternYDv);
            }
            nodeQueue.add(downStreamOp);
          }

          if (currentNode.isSinkOperator) {
            sinksList.add(patternYDv);
          }
        }
      }

      // set objective function - this sets the maximization objective of the function (we want to maximize the sum of all sinks' outputs)
      MPObjective objective = solver.objective();
      sinksList.forEach(dv -> objective.setCoefficient(dv, 1));
      objective.setMaximization();

      // solve LP
      MPSolver.ResultStatus results = solver.solve();

      // append solution values for all x-dvs, serialize them and forward them to the overloaded operator
      StringBuilder output = new StringBuilder();
      output.append(overloadedOperatorInfo.name).append(":");
      if (results != MPSolver.ResultStatus.OPTIMAL) {
        xDvList.forEach(xDv -> output.append(xDv.name()).append("|").append(1).append(":"));
      } else {
        xDvList.forEach(xDv -> output.append(xDv.name()).append("|").append(xDv.solutionValue()).append(":"));
      }
      ctx.output(sosOutput, output.toString());

      /*
      following 12 are only for debugging - use a Kafka consumer on the topic globalOut to
      see the coordinator's outputs. If you are not seeing anything then do check the
      latency bound at Settings.java it set high by standard.
       */
      output.append("\nResults: ").append(results);
      output.append("\nSolution: ").append(objective.value());
      output.append("\nSinks: \n");
      sinksList.forEach(dv -> output.append(dv.name()).append(": ").append(dv.solutionValue()).append("\n"));
      output.append("\nDvs: \n");
      patternsDvs.forEach((key, value1) -> output.append(value1.name()).append(": ").append(value1.solutionValue()).append("\n"));
      output.append("\nConstraints: \n");
      Arrays.stream(solver.constraints()).map(cst -> String.format("%s\n", cst.name())).forEach(output::append);
      output.append("\nProcessing Time: \n");
      output.append("p: " + p + " pstar: " + pStar).append("\n");


      // end work - clear information from the OperatorInfo instances and fetch the next job if available
      operatorsList[indexer.get(lastOverloadedOperator)].isOverloaded = false;
      out.collect(lastOverloadedOperator + " " + value.id + ": Ready with:\n" + this + "\n" + output);
      clear();
      jobQueue.remove(lastOverloadedOperator);
      if (!jobQueue.isEmpty()) {
        operatorsList[indexer.get(jobQueue.iterator().next())].isOverloaded = true;
        ctx.output(sosOutput, "snap:" + System.nanoTime());
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Coordinator that = (Coordinator) o;
    return Objects.equals(indexer, that.indexer) && Objects.deepEquals(operatorsList, that.operatorsList);
  }

  public void clear() {
    for (OperatorInfo operator : operatorsList) {
      operator.clear();
    }
  }


  @Override
  public int hashCode() {
    return Objects.hash(indexer, Arrays.hashCode(operatorsList));
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (String index : indexer.keySet()) {
      result.append(index).append("\n");
      result.append(operatorsList[indexer.get(index)]).append("\n");
    }
    return result.toString();
  }
}
