package org.uni.potsdam.p1.actors;

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
import java.util.concurrent.TimeUnit;

public class Coordinator extends KeyedProcessFunction<Long, Metrics, String> {

  public HashMap<String, Integer> indexer;
  public OperatorInfo[] operatorsList;
  public ValueState<Metrics> lambda;
  public Set<String> jobQueue;
  public OutputTag<String> sosOutput;
  public Set<String> sinkOperators;

  // TODO find a proper latency bound
  public static double LATENCY_BOUND = 15.15E-6;

  public Coordinator() {
  }

  public Coordinator(OutputTag<String> sosOutput, OperatorInfo... operators) {
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
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    lambda = getRuntimeContext().getState(new ValueStateDescriptor<>("lambda", Metrics.class));
  }

  @Override
  public void processElement(Metrics value, KeyedProcessFunction<Long, Metrics, String>.Context ctx, Collector<String> out) throws Exception {
//    out.collect("Got: " + value + " lambda:" + (lambda.value() == null ? "null" : lambda.value().id) + " id: " + value.id);
    if (value.description.equals("overloaded")) {
      if (jobQueue.isEmpty()) {
        operatorsList[indexer.get(value.name)].isOverloaded = true;
        ctx.output(sosOutput, "snap:" + System.nanoTime());
      }
      jobQueue.add(value.name);
    } else {
      operatorsList[indexer.get(value.name)].put(value.description, value);
      if (value.name.matches("(o1|o2)") && value.description.equals("lambdaOut")) {
        if (lambda.value() == null) {
          lambda.update(value);
        } else {
          try {
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
          } catch (NullPointerException e) {
            StringBuilder output = new StringBuilder();
            output.append(this).append("\n");
            Optional.ofNullable(lambda.value()).ifPresent(output::append);
            output.append("\n").append(value);
            out.collect(output.toString());
            TimeUnit.SECONDS.sleep(3);
            throw new RuntimeException(e);
          }
        }
      }
    }
    boolean isReady = true;
    for (OperatorInfo operator : operatorsList) {
      isReady &= operator.isReady();
    }
    if (isReady) {
      // define overloaded operator
      String lastOverloadedOperator = jobQueue.iterator().next();
      OperatorInfo overloadedOperatorInfo = operatorsList[indexer.get(lastOverloadedOperator)];

      // import libraries
      Loader.loadNativeLibraries();

      // declare solver
      MPSolver solver = MPSolver.createSolver("GLOP");

      // create decision variables for the overloaded operator
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

      // create decision variables for the direct outputs of the decision operator
      Map<String, MPVariable> decisionVariablesY = new HashMap<>(numberOfPatterns);
      Map<String, MPVariable> decisionVariablesSinks = new HashMap<>(sinkOperators.size());
      for (int i = 0; i < numberOfPatterns; i++) {
        String pattern = overloadedOperatorInfo.patterns[i].name;
        MPVariable yDv = solver.makeNumVar(0, overloadedOperatorInfo.get("mu").get("total"), "y_" + pattern);
        decisionVariablesY.put(pattern, yDv);
        if (overloadedOperatorInfo.isSinkOperator) {
          decisionVariablesSinks.put(pattern, yDv);
        }
      }

      // create decision variable for the sinks - use operator's name as key
      for (String sinkName : sinkOperators) {
        if (sinkName.equals(overloadedOperatorInfo.name)) {
          continue;
        }
        OperatorInfo sinkOperator = operatorsList[indexer.get(sinkName)];
        decisionVariablesSinks.put(sinkName, solver.makeNumVar(0, sinkOperator.get("mu").get("total"), "sink_" + sinkName));
      }

      double infinity = Double.POSITIVE_INFINITY;
      // set constraints for the selectivity functions
      for (EventPattern pattern : overloadedOperatorInfo.patterns) {
        // get pattern name of the last decisionVariable passed to this method
        String[] patternInfo = pattern.type.split(":");
        switch (patternInfo[0]) {
          case "AND": {
            for (int i = 1; i < patternInfo.length; i++) {
              String inputType = patternInfo[i];
              double bound = overloadedOperatorInfo.get("lambdaIn").get(inputType);
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
              double bound = overloadedOperatorInfo.get("lambdaIn").get(inputType);
              MPConstraint constraint = solver.makeConstraint(-infinity, bound, "y_" + pattern.name + "_" + inputType);
              constraint.setCoefficient(Objects.requireNonNull(decisionVariablesY.get(pattern.name)), Double.parseDouble(typeInfo.substring(separator + 1)));
              constraint.setCoefficient(Objects.requireNonNull(decisionVariablesX.get(pattern.name + "_" + inputType)), bound);
            }
            break;
          }
          case "OR": {
            for (int i = 1; i < patternInfo.length; i++) {
              String inputType = patternInfo[i];
              double bound = overloadedOperatorInfo.get("lambdaIn").get(inputType);
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
            Objects.requireNonNull(operator.get("lambdaIn").get(input)), "sink_" + input);
          constraint.setCoefficient(decisionVariablesSinks.get(sink), 1);
        }
      }

      // declare time constraint
      double totalInputRate = overloadedOperatorInfo.get("lambdaIn").get("total");
      double totalProcessingTime = overloadedOperatorInfo.get("ptime").get("total");
      double p = Arrays.stream(overloadedOperatorInfo.inputTypes).map(inputType -> (overloadedOperatorInfo.get("lambdaIn").get(inputType) / totalInputRate) * totalProcessingTime).reduce(0., Double::sum);
      double pStar = 1 / ((1 / LATENCY_BOUND) + totalInputRate);

      MPConstraint timeConstraint = solver.makeConstraint(p - pStar, MPSolver.infinity(), "time_constraint");
      double ptime = overloadedOperatorInfo.get("ptime").get(overloadedOperatorInfo.patterns[0].name);
      for (int i = 0, indexPatterns = 0; i < decisionVariablesX.size(); i++) {
        String inputType = overloadedOperatorInfo.inputTypes[i % numberOfInputs];
        double factor = (overloadedOperatorInfo.get("lambdaIn").get(inputType) / totalInputRate) * ptime;
        MPVariable dv = Objects.requireNonNull(decisionVariablesX.get(overloadedOperatorInfo.patterns[indexPatterns].name + "_" + inputType));
        timeConstraint.setCoefficient(dv, factor);
        if (i == numberOfInputs - 1 && indexPatterns < overloadedOperatorInfo.patterns.length - 1) {
          ptime = overloadedOperatorInfo.get("ptime").get(overloadedOperatorInfo.patterns[++indexPatterns].name);
        }
      }

      // set objective function
      MPObjective objective = solver.objective();
      decisionVariablesSinks.values().forEach(dv -> objective.setCoefficient(dv, 1));
      objective.setMaximization();

      // solve LP
      MPSolver.ResultStatus results = solver.solve();

      StringBuilder output = new StringBuilder();
      // main output for the coordinator
      output.append(overloadedOperatorInfo.name).append(":");
      for (int i = 0, indexPatterns = 0; i < numberOfInputs * numberOfPatterns; i++) {
        String variableName = overloadedOperatorInfo.patterns[indexPatterns].name + "_" + overloadedOperatorInfo.inputTypes[i % numberOfInputs];
        output.append(variableName).append("|").append(decisionVariablesX.get(variableName).solutionValue()).append(":");
        if (i == numberOfInputs - 1) {
          indexPatterns++;
        }
      }
      ctx.output(sosOutput, output.toString());

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

//      output.append("\nTime Constraints: \n");
//      decisionVariablesX.values().forEach(dv -> output.append(dv.name()).append(": ").append(timeConstraint.getCoefficient(dv)).append("\n"));

      // end work
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

  public void fillConstraintList(OperatorInfo currentOperator, MPVariable lastDecisionVariable, List<MPConstraint> contraintList, MPSolver solver) {

    // get pattern name of the last decisionVariable passed to this method
    String variableName = lastDecisionVariable.name();
    String typeName = variableName.substring(variableName.indexOf('_') + 1);

    // iterate over the different patterns implemented by the current operator
    for (EventPattern pattern : currentOperator.patterns) {

      // determine the type of pattern being implemented
      String[] information = pattern.type.split(":");

//      MPVariable patternOutputVariable = solver.makeNumVar(0,currentOperator.get("mu").get(pattern.name),)
      if (information[0].equals("AND")) {
        for (int i = 1; i < information.length; i++) {
          String currentType = information[i];
          MPConstraint outputConstraint = solver.makeConstraint(Double.NEGATIVE_INFINITY, currentOperator.get("lambdaIn").get(currentType), "y_" + pattern);
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
