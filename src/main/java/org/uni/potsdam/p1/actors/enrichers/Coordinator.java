package org.uni.potsdam.p1.actors.enrichers;

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

      // TODO: improve
      // join the output rates of operators o1 and o2 manually
      if (value.name.matches("(o1|o2)") && value.description.equals("lambdaOut")) {
        if (lambda.value() == null) {
          lambda.update(value);
        } else {
          Metrics op1 = value.name.equals("o1") ? value : lambda.value();
          Metrics op2 = value == op1 ? lambda.value() : value;
          Metrics op3 = new Metrics("o3", "lambdaIn", 3);
          Metrics op4 = new Metrics("o4", "lambdaIn", 3);
          Double lambda11 = op1.get("11");
          Double lambda12 = op1.get("12");
          Double lambda21 = op2.get("21");
          Double lambda22 = op2.get("22");
          op3.put("11", lambda11);
          op3.put("21", lambda21);
          op3.put("total", lambda21 + lambda11);
          op4.put("12", lambda12);
          op4.put("22", lambda22);
          op4.put("total", lambda22 + lambda12);
          operatorsList[indexer.get("o3")].put("lambdaIn", op3);
          operatorsList[indexer.get("o4")].put("lambdaIn", op4);
          lambda.clear();
        }
      }
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

      // create decision variables for the overloaded operator (creates x-dvs for every input type of every pattern in the range [0,1])
      int numberOfInputs = overloadedOperatorInfo.inputTypes.length;
      int numberOfPatterns = overloadedOperatorInfo.patterns.length;
      Map<String, MPVariable> decisionVariablesX = new HashMap<>(numberOfInputs * numberOfPatterns);
      for (int i = 0, indexPatterns = 0; i < numberOfInputs * numberOfPatterns; i++) {
        String variableName = overloadedOperatorInfo.patterns[indexPatterns].name + "_" + overloadedOperatorInfo.inputTypes[i % numberOfInputs];
        decisionVariablesX.put(variableName, solver.makeNumVar(0., 1., "x_" + variableName));
        if (i == numberOfInputs - 1) {
          indexPatterns++;
        }
      }

      // create decision variables for the direct outputs of the decision operator (creates y-dvs for every pattern in the range [0,processing rate] - due to the output constraint)
      Map<String, MPVariable> decisionVariablesY = new HashMap<>(numberOfPatterns);
      Map<String, MPVariable> decisionVariablesSinks = new HashMap<>(sinkOperators.size());
      for (int i = 0; i < numberOfPatterns; i++) {
        String pattern = overloadedOperatorInfo.patterns[i].name;
        MPVariable yDv = solver.makeNumVar(0, overloadedOperatorInfo.getValue("mu", "total"), "y_" + pattern);
        decisionVariablesY.put(pattern, yDv);
        if (overloadedOperatorInfo.isSinkOperator) {
          decisionVariablesSinks.put(pattern, yDv);
        }
      }

      // create decision variable for the sinks - use operator's name as key (these are the y-dvs of the system in the range [0,processing rate] - output constraint)
      for (String sinkName : sinkOperators) {
        if (sinkName.equals(overloadedOperatorInfo.name)) {
          continue;
        }
        OperatorInfo sinkOperator = operatorsList[indexer.get(sinkName)];
        decisionVariablesSinks.put(sinkName, solver.makeNumVar(0, sinkOperator.getValue("mu", "total"), "sink_" + sinkName));
      }

      double infinity = Double.POSITIVE_INFINITY;

      //TODO improve this - the method described bellow only creates selectivity-function constraints for the y-dvs of the output operator and for the sinks, it doesn't travel across the operator graph

      // set constraints for the selectivity functions. Matches a pattern type to a selectivity function's constraint
      for (EventPattern pattern : overloadedOperatorInfo.patterns) {
        // getMetric pattern name of the last decisionVariable passed to this method
        String[] patternInfo = pattern.type.split(":");
        switch (patternInfo[0]) {
          case "AND": {
            for (int i = 1; i < patternInfo.length; i++) {
              String inputType = patternInfo[i];
              double bound = overloadedOperatorInfo.getValue("lambdaIn", inputType);
              MPConstraint constraint = solver.makeConstraint(-infinity, bound, "y_" + pattern.name + "_" + inputType);
              constraint.setCoefficient(Objects.requireNonNull(decisionVariablesY.get(pattern.name)), 1);
              constraint.setCoefficient(Objects.requireNonNull(decisionVariablesX.get(pattern.name + "_" + inputType)), bound);
            }
            break;
          }
          case "SEQ": {
            for (int i = 1; i < patternInfo.length; i++) {
              String typeInfo = patternInfo[i];
              int separator = typeInfo.indexOf("|");
              String inputType = typeInfo.substring(0, separator);
              double bound = overloadedOperatorInfo.getValue("lambdaIn", inputType);
              MPConstraint constraint = solver.makeConstraint(-infinity, bound, "y_" + pattern.name + "_" + inputType);
              constraint.setCoefficient(Objects.requireNonNull(decisionVariablesY.get(pattern.name)), Double.parseDouble(typeInfo.substring(separator + 1)));
              constraint.setCoefficient(Objects.requireNonNull(decisionVariablesX.get(pattern.name + "_" + inputType)), bound);
            }
            break;
          }
          case "OR": {
            for (int i = 1; i < patternInfo.length; i++) {
              String inputType = patternInfo[i];
              double bound = overloadedOperatorInfo.getValue("lambdaIn", inputType);
              MPConstraint constraint = solver.makeConstraint(bound, infinity, "y_" + pattern.name + "_" + inputType);
              constraint.setCoefficient(Objects.requireNonNull(decisionVariablesY.get(pattern.name)), 1);
              constraint.setCoefficient(Objects.requireNonNull(decisionVariablesX.get(pattern.name + "_" + inputType)), bound);
            }
          }
        }
        if (pattern.downstreamOperators != null) {
          for (String downStreamOp : pattern.downstreamOperators) {
            MPConstraint constraint = solver.makeConstraint(-infinity, 0, "sink_" + pattern.name);
            constraint.setCoefficient(Objects.requireNonNull(decisionVariablesSinks.get(downStreamOp)), 1);
            constraint.setCoefficient(Objects.requireNonNull(decisionVariablesY.get(pattern.name)), -1);
          }
        }
      }

      // apply selectivity function constraints for the sinks
      for (String sink : sinkOperators) {
        if (overloadedOperatorInfo.name.equals(sink)) {
          continue;
        }
        OperatorInfo operator = operatorsList[indexer.get(sink)];
        for (String input : operator.inputTypes) {
          if (overloadedOperatorInfo.hasPattern(input)) {
            continue;
          }
          MPConstraint constraint = solver.makeConstraint(-infinity,
            Objects.requireNonNull(operator.getMetric("lambdaIn").get(input)), "sink_" + input);
          constraint.setCoefficient(decisionVariablesSinks.get(sink), 1);
        }
      }

      /*
       * declare time constraint -> calculate average processing time for the given
       * snapshot and measure it against the ideal value p* (calculated with the latency bound and totalInputRate at the overloaded operator)
       */
      double totalInputRate = overloadedOperatorInfo.getMetric("lambdaIn").get("total");
      double totalProcessingTime = overloadedOperatorInfo.getMetric("ptime").get("total");
      double p = Arrays.stream(overloadedOperatorInfo.inputTypes).map(inputType -> (overloadedOperatorInfo.getMetric("lambdaIn").get(inputType) / totalInputRate) * totalProcessingTime).reduce(0., Double::sum);
      double pStar = 1 / ((1 / bound) + totalInputRate);

      // this uses Equation 3 of the paper ( it just changes the equation to x)
      MPConstraint timeConstraint = solver.makeConstraint(p - pStar, MPSolver.infinity(), "time_constraint");
      double ptime = overloadedOperatorInfo.getMetric("ptime").get(overloadedOperatorInfo.patterns[0].name);
      for (int i = 0, indexPatterns = 0; i < decisionVariablesX.size(); i++) {
        String inputType = overloadedOperatorInfo.inputTypes[i % numberOfInputs];
        double factor = (overloadedOperatorInfo.getMetric("lambdaIn").get(inputType) / totalInputRate) * ptime;
        MPVariable dv = Objects.requireNonNull(decisionVariablesX.get(overloadedOperatorInfo.patterns[indexPatterns].name + "_" + inputType));
        timeConstraint.setCoefficient(dv, factor);
        if (i == numberOfInputs - 1 && indexPatterns < overloadedOperatorInfo.patterns.length - 1) {
          ptime = overloadedOperatorInfo.getMetric("ptime").get(overloadedOperatorInfo.patterns[++indexPatterns].name);
        }
      }

      // set objective function - this sets the maximization objective of the function (we want to maximize the sum of all sinks' outputs)
      MPObjective objective = solver.objective();
      decisionVariablesSinks.values().forEach(dv -> objective.setCoefficient(dv, 1));
      objective.setMaximization();

      // solve LP
      MPSolver.ResultStatus results = solver.solve();

      // append solution values for all x-dvs, serialize them and forward them to the overloaded operator
      StringBuilder output = new StringBuilder();
      output.append(overloadedOperatorInfo.name).append(":");
      for (int i = 0, indexPatterns = 0; i < numberOfInputs * numberOfPatterns; i++) {
        String variableName = overloadedOperatorInfo.patterns[indexPatterns].name + "_" + overloadedOperatorInfo.inputTypes[i % numberOfInputs];
        output.append(variableName).append("|").append(decisionVariablesX.get(variableName).solutionValue()).append(":");
        if (i == numberOfInputs - 1) {
          indexPatterns++;
        }
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
      decisionVariablesSinks.values().forEach(dv -> output.append(dv.name()).append(": ").append(dv.solutionValue()).append("\n"));
      output.append("\nOutputs: \n");
      decisionVariablesY.values().forEach(dv -> output.append(dv.name()).append(": ").append(dv.solutionValue()).append("\n"));
      output.append("\nInputs: \n");
      decisionVariablesX.values().forEach(dv -> output.append(dv.name()).append(": ").append(dv.solutionValue()).append("\n"));
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

  //TODO work in progress - should traverse the operator graph Depth first and generate decision variables on the way
  public void fillConstraintList(OperatorInfo currentOperator, MPVariable lastDecisionVariable, List<MPConstraint> contraintList, MPSolver solver) {

    // getMetric pattern name of the last decisionVariable passed to this method
    String variableName = lastDecisionVariable.name();
    String typeName = variableName.substring(variableName.indexOf('_') + 1);

    // iterate over the different patterns implemented by the current operator
    for (EventPattern pattern : currentOperator.patterns) {

      // determine the type of pattern being implemented
      String[] information = pattern.type.split(":");

//      MPVariable patternOutputVariable = solver.makeNumVar(0,currentOperator.getMetric("mu").getMetric(pattern.name),)
      if (information[0].equals("AND")) {
        for (int i = 1; i < information.length; i++) {
          String currentType = information[i];
          MPConstraint outputConstraint = solver.makeConstraint(Double.NEGATIVE_INFINITY, currentOperator.getMetric("lambdaIn").get(currentType), "y_" + pattern);
          if (information[i].equals(typeName)) {
            outputConstraint.setCoefficient(lastDecisionVariable, -1);
            outputConstraint.setCoefficient(lastDecisionVariable, -1);
          }
        }
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
