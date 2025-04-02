package org.uni.potsdam.p1.heuristic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.TargetMember;
import org.uni.potsdam.p1.types.outputTags.StringOutput;

public class HybridAnalyser extends CoProcessFunction<Metrics, String, String> {
    
    // Operator information
    public String opName;
    public OperatorInfo operator;
    
    // metrics
    public Map<String, Double> newInputRates;
    public Map<String,Double> oldInputRates;
    public Map<String,Double> outputRates;
    public Map<String, Double> processingTimes; 
    public Map<String,Double> selectivities;

    public boolean isShedding = false;
    public boolean isOverloaded;
    public boolean collecting = false;
    public boolean inputRateIsReady = false;
    public boolean outputRateIsReady = false;
    public boolean ptimesAreReady = false;

    //public double currentAveragePtime;
    //public double lastAveragePTime;
    //public double currentTotalInputRate;
    public double gradualChanges;
    //public double lastTotalInputRate;
    public long traversalInitiation;

    // outsource these aparameters into the Settings classes maybe
    public long timeout;
    public long cooldown;
    public long lifeTime;
    public int messageID;

    public double lastLambda;
    public double lastPtime;
    public double lastAverage;

    // fields for shedding calculation
    public LoadShedSelector shedder;
    public double bound;
    public double pStar;
    //public Metrics sheddingRates;
    
    // maybe event implement a seperate parser
    public Pattern headerPattern;
    public Pattern regPattern;
    
    // Kafka output tag
    public StringOutput toKafka;
    // probably better use an ArrayList?
    public Map<String, Set<TargetMember>> target;
    public Map<String, StringOutput> tagList; 
    public Map<String, ArrayList<String[]>> inputToOutput;

    private static final Logger aLog = LoggerFactory.getLogger(HybridAnalyser.class);

    // for flink type extraction
    public HybridAnalyser(){}

    public HybridAnalyser(OperatorInfo operator) {
        this.operator = operator;
        this.opName = operator.name;
        this.isOverloaded = operator.isOverloaded;

        // TODO implement adaptive timeouts
        timeout = 3000;
        cooldown = timeout + 1000;
        lifeTime = 500;
        //messageID = (int) Math.random() * 1001 ;
        messageID = 0;

        // Initialize metrics holders including totals
        //sheddingRates = new Metrics(operator.name, "shares", operator.outputTypes.length * operator.inputTypes.length + 1);
        selectivities = new HashMap<String, Double>(operator.patterns.length);
        inputToOutput = new HashMap<String, ArrayList<String[]>>(operator.inputTypes.length);
        processingTimes = new HashMap<String, Double>(operator.inputTypes.length + 1);
        oldInputRates = new HashMap<String, Double>(operator.inputTypes.length + 1);
        newInputRates = new HashMap<String, Double>(operator.inputTypes.length + 1);
        outputRates = new HashMap<String, Double>(operator.outputTypes.length + 1);
        target = new HashMap<String, Set<TargetMember>>(operator.patterns.length);
        
        // Initialize baseline values
        //lastAveragePTime = operator.latencyBound * 0.9;
        //lastTotalInputRate = operator.controlBatchSize;
        
        headerPattern = Pattern.compile("target:([^\\s]+) description:([^\\s]+) origin:([^\\s]+) pattern:([^\\s]+) timestamp:([^\\s]+) messageID:([^\\s]+)");
        regPattern = Pattern.compile(" outputType:([^\\s]+) value:([-+]?[0-9]*\\.?[0-9]+)");
        
        //maybe outsoure this to the EventPattern class itself
        // initialize! IllegalStateException
        tagList = new HashMap<>();
        
        for (EventPattern currentPattern : operator.patterns) {
            
            // create map that stores to which downstream Operators we have to forward a msg
            // the map stores an array of downstreamOp arrays for every input of the operator
            String[] currentInputTypes = currentPattern.getParameters();
            
            for (String inputType : currentInputTypes) {
                ArrayList<String[]> entry;
                String[] dso = currentPattern.downstreamOperators;
             
                if (!inputToOutput.containsKey(inputType)) {
                    entry = new ArrayList<String[]>();
                    inputToOutput.put(inputType, entry);
                } else {
                    entry = inputToOutput.get(inputType);
                }
             
                String[] downstreamOpSet = new String[dso.length + 1];
                // outputType for to correctly pack messages always at first position
                downstreamOpSet[0] = currentPattern.name;
                int j = 1;
                for (int i = 0; i < dso.length; i++) {
                    downstreamOpSet[j] = dso[i];
                    j++;
                }
                entry.add(downstreamOpSet);
                inputToOutput.put(inputType, entry);      
            }
            for (String downstreamOp : currentPattern.downstreamOperators) {
                // I don't think we need check if key in Map, bc it would just overwrite
                // the value, if the key is contained, which is faster than searching the whole map for the key each time
                tagList.put(downstreamOp, new StringOutput("to_analyser_" + downstreamOp));
            }
        }
        
        //aLog.info("Size of input to output mappings:" + inputToOutput.size());

        for (String key : operator.outputTypes) {
            processingTimes.put(key, -1.);
            selectivities.put(key, -1.);
            outputRates.put(key, -1.);
        }
        
        for (String lambdaKey : operator.inputTypes) {
            newInputRates.put(lambdaKey, -1.);
            oldInputRates.put(lambdaKey, -1.);
        }
        
        /*
        for (String inputType : operator.inputTypes) {
            for (String outType : operator.outputTypes) {
                sheddingRates.put(outType + "_" + inputType, 0.);
            }
        }
        */

        processingTimes.put("total", -1.);
        newInputRates.put("total", -1.);
        outputRates.put("total", -1.);
        oldInputRates.put("total", -1.);
        selectivities.put("birth", -1.);
        
        bound = operator.latencyBound;
        shedder = new LoadShedSelector(operator);
        

        lastAverage = operator.latencyBound;
        lastPtime = lastAverage;
        lastLambda = operator.controlBatchSize;
    }

    public void resetDataStructures() {
        for (String key : operator.outputTypes) {
            selectivities.put(key, -1.);
        }
        
        for (String inputType : operator.inputTypes) {
            for (String outType : operator.outputTypes) {
                shedder.sheddingRates.put(outType + "_" + inputType, 0.);
            }
        }
        selectivities.put("birth", -1.);
    }

    /**
     * Overload detection and shedder communication as well as SOS report to downstream operators
     * <bl>
     * This function checks:
     * <ul>
     *      <li> Is the operator currently overloaded based on a change in the stream characteristics </li>
     *      <li> Will the operator overload based on ptimes, that are dangerously close to the operators bound and future inputRates </li>
     * </ul>
     * <p>
     * In case of an overload the function reports its latest inputRates, outputRates and
     * processingTimes to the current load shedder. It also updates its selectivities and
     * reports them as well. Simulatenously it creates an SOS message in the shape of
     * the packed selectivities and thus, initiates the traversal algorithm. 
     * In this function, the target and origin of the selectivity sos message is always the 
     * current operator. This initiates the traversal.
     * </p>
     * 
     * @see org.apache.flink.streaming.api.functions.co.CoProcessFunction
     * @param metric The metric input type we are processing in this stream
     * @param ctx Context object, that enables us specifically to send via SideOutputs
     * @param out Collecter object, that lets us send messages to the main output
     */
    @Override
    public void processElement1(Metrics metric, Context ctx, Collector<String> out) {
        boolean coldStartAvoided = inputRateIsReady && outputRateIsReady && ptimesAreReady;

        switch (metric.description) {
            case "ptime":
                ptimesAreReady = true;
                processingTimes = metric.map;
                /*
                if (!collecting) {
                    processingTimes = metric.map;
                }
                */

                break;
            case "lambdaOut":
                outputRateIsReady = true;
                if (metric.name.equals(opName) && metric != null /*&& !collecting*/) {
                    outputRates = metric.map;
                    break;
                }
            
            case "lambdaIn":
                inputRateIsReady = true;
                // too lazy to remove the condition but want to keep the idead for now
                if (!collecting || collecting) {
                    double diff = 0;
                    // do we run over all eventTypes??
                    // don't we add inputRates for events that arent in our current operator
                    for(String eventType :  newInputRates.keySet()) {
                        if(!eventType.equals("total")) {
                        double oldValue = newInputRates.get(eventType); 
                        double newValue = metric.get(eventType);
                        diff += (newValue-oldValue);
                        newInputRates.put(eventType, newValue); 
                        oldInputRates.put(eventType, oldValue);
                        }
                    }
                    var oldTotal = newInputRates.get("total");
                    newInputRates.put("total",newInputRates.get("total")+diff);
                    oldInputRates.put("total", oldTotal);
                }
                break;                
            }

            boolean cooledDown = true;

            if (collecting) {
                long currentTime = System.currentTimeMillis();
                long timeInterval = currentTime - traversalInitiation;
                cooledDown = currentTime - traversalInitiation > cooldown;
                if (timeInterval > timeout) {
                    collecting = false;
                }
            }

            // I think we shall be careful here:
            // because even if we freeze the metrics to the time of detecting the overload
            // we still get processingTimes smaller than the optimal time.

            // we can cut down processing time here by just checking overload in time intervals instead of every metric receival
            if (coldStartAvoided) {
                double totalLambda = newInputRates.get("total");
                double totalPtime = processingTimes.get("total");
                double calculatedP = 0.;

                for (String key : operator.inputTypes) {
                    double weight = 0;
                        for (String key2 : operator.outputTypes) {
                            double share = isShedding ? (1 - shedder.sheddingRates.get(key2 + "_" + key)) : 1;
                            weight += share * processingTimes.get(key2);
                        }
                calculatedP += (newInputRates.get(key) / (totalLambda == 0 ? 1 : totalLambda)) * weight;
                }


                double B = Math.max(0, (1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - totalLambda)));
                boolean pHasChanged = calculatedP > 1.1 * lastAverage;
                boolean lambdaHasChanged = totalLambda > 1.05 * lastLambda;
                boolean ptimeHasChanged = totalPtime > 1.05 * lastPtime;

                double upperBound = 1 / ((1 / bound) + totalLambda);
                double lowerBound = upperBound * 0.95;
                double sheddingExit = upperBound * 0.85;
                
                if (calculatedP >= lowerBound || ((pHasChanged || lambdaHasChanged || ptimeHasChanged) && B > bound) && coldStartAvoided) {
                    if (/*!isShedding &&*/!collecting && !isOverloaded) {
                        overloadDetected(ctx, out);
                        lastAverage = calculatedP;
                        lastPtime = totalPtime;
                        lastLambda = totalLambda;
                        return;
                    } 
                // when do we want to stop shedding really? Because if we stop prematurely in the traversal, we have to synchronize this overall
                //if we are collecting, we always set the shedding flag, so the check is not really necessary I think
                } else if (isShedding && (totalPtime < sheddingExit) && !collecting && cooledDown && coldStartAvoided) {
                    recoverFromOverload(ctx);
                    lastAverage = calculatedP;
                    lastPtime = totalPtime;
                    lastLambda = totalLambda;
                    return;
                }
            }
        }   


    /**
     * Calculate the selectivities and store via side effects.
     * This is ONLY DONE, when the operator overloads
     * The selectivities result from pattern-level min/max operations on the inputs
     * 
     * @throws IllegalArgumentException
     */
    public void updateSelectivities() throws IllegalArgumentException{
        //aLog.info("updating selectivities in " + opName + "analyzer");
        try {

            for (EventPattern pattern : operator.patterns) {
                Map<String, Integer> eventFrequency = pattern.getWeightMaps();
                String name = pattern.name;
                String pType = pattern.getType();
                
                if (pType.equals("AND") || pType.equals("SEQ")) {
                    double currentSel = selectivities.get(name) == 0 || selectivities.get(name) == -1 || !selectivities.containsKey(name) ? Double.POSITIVE_INFINITY : selectivities.get(name);
                    
                    for (String eventType : eventFrequency.keySet()) {
                        currentSel = newInputRates.get(eventType) > 0 ? Math.min(newInputRates.get(eventType) / eventFrequency.get(eventType), currentSel) : 0.;
                        //aLog.info("InputRate in calculation in analyzer " + opName + "with inputRate: " + newInputRates.get(eventType));
                    }
                    this.selectivities.put(name, currentSel);
                    //aLog.info("Calculated selectivity: " + currentSel + " for input: " + name + "with inputRate: ");
                }
                
                else if (pType.equals("OR")) {
                    double currentSelOr = selectivities.get(name) == 0 || selectivities.get(name) == -1 || !selectivities.containsKey(name) ? Double.NEGATIVE_INFINITY : selectivities.get(name);
                    for (String eventType : eventFrequency.keySet()) {
                        currentSelOr = Math.max(newInputRates.get(eventType) / eventFrequency.get(eventType), currentSelOr);
                    }
                    this.selectivities.put(name, currentSelOr);
                    //aLog.debug(String.format("%6f", currentSelOr));
                    //aLog.info("Calculated selectivity: " + currentSelOr + " for input: " + name);
                }
            }
        } catch(Exception e) {
            aLog.info("Error in " + opName + "as: " + e);
            throw new IllegalArgumentException("Likelypattern type unknown in " + opName);
        }  

        if (operator.isSinkOperator) {
            aLog.warn("The sink " + opName + "has stats: " + processingTimes.toString() + newInputRates.toString() + outputRates.toString() + selectivities.toString());
        } else {
            aLog.info("Local selectivities in " + opName + " of: " + selectivities.toString());
        }
        selectivities.put("birth", Double.valueOf(System.currentTimeMillis()));
        aLog.info("Successfully updates selectivities in " + opName + " as: " + selectivities.toString());
            //var res = this.selectivities.put("birth", Double.valueOf(System.currentTimeMillis()));
            //aLog.info("Processing times in " + opName + parseMapToString(processingTimes, opName, opName, opName));
            //aLog.info("InputRates in " + opName + parseMapToString(newInputRates, opName, opName, opName));
            //aLog.info("OutputRates in " + opName + parseMapToString(outputRates, opName, opName, opName));
            //aLog.info("Selectivities in " + opName + parseMapToString(selectivities, opName, opName, opName));
        }

    public void recoverFromOverload(Context ctx) {
        String stopMsg = String.format("target:%s description:stopShedding origin:%s pattern:null timestamp:null messageID:%d ~ null", opName, opName, 0);
        //we delete the sheddingRates but delete the selectivities after they die
        // sheddingRates.map.clear();
        for (String inputType : operator.inputTypes) {
            for (String outType : operator.outputTypes) {
                shedder.sheddingRates.put(outType + "_" + inputType, 0.);
            }
        }

        aLog.info("overload resolved in " + opName + " sending: " + stopMsg);
        ctx.output(toKafka, stopMsg);
        isOverloaded = false;
        isShedding = false;
    }
    
    /**
     * Initiates the traversal algorithm by sending local selectivities to the corresponding downstream operators.
     * 
     * @param ctx Context object
     */
    public void initiateDownStreamTraversal(Context ctx) {
        for (EventPattern pattern : operator.patterns) {
            String patternName = pattern.name;
            this.traversalInitiation = System.currentTimeMillis();
            long currentTime = System.currentTimeMillis();
            messageID = (int) traversalInitiation / 10_000_000;
            for (String downstreamOp : pattern.downstreamOperators) {
                // since we detect the overload, the target for the selectivities is us
                String message = parseSelToString(selectivities.get(patternName), operator.name, currentTime, patternName, patternName, messageID);
                
                if (tagList.containsKey(downstreamOp)) {
                    StringOutput sideOut = tagList.get(downstreamOp); 
                    ctx.output(sideOut, message);
                }
            }
        }
    }

    /**
     * Procedure to send shedding information to the shedder. 
     * The traversal will be initiated by sending the selectivities downstream
     * to the relevant operators.
     * 
     * @param ctx Context Object we use to send data via SideOutputs
     * @param out Collecor, that collects messages in order to ship them via the main output
     */
    public void overloadDetected(Context ctx, Collector<String> out) {
        try {
            isOverloaded = true;
            isShedding = true;
            //selectivities.clear();
            //sheddingRates.map.clear();
            resetDataStructures();
            //aLog.info("overload detected in " + opName);
            updateSelectivities();

            reportToKafka(ctx);
            aLog.info("Overload detected from overload coordination function in" + opName + ": inputR: " + newInputRates + " ouputR: " + outputRates + " pTimes: " + processingTimes + "shedding Rates: " + shedder.sheddingRates.toString());
            
            // if we are the leaf operator that overloads
            // leave it at sending local sheddingRates to the operator (local shedding only)
            if (operator.isSinkOperator) {
                //isShedding = false;
                return;
            } else {
                collecting = true;
                initiateDownStreamTraversal(ctx);

                //collceting = true;
            }

        }
        catch (Exception e) {
            aLog.error("Exception in overload deteciton of type:" + e);
            throw new RuntimeException(e);
        }
    }


    /**
     * A report function, that sends the initial sheddingRates to the operator.
     * This is only for local communication, not for communication with other operators.
     * <p>
     * @param ctx object to use in order to send messages to the Kafka SideOutput
     * </p>
     * Problem: Message might be too long, might have to send in chunks?
     */
    public void reportToKafka(Context ctx) {
        try {
            boolean shouldSend = shedder.calculateSheddingRates(isShedding, this.newInputRates, this.outputRates, this.processingTimes, this.selectivities);
            aLog.info("Sending local sheddingRates in " + opName);

            String initMsg = String.format("target:%s description:startShedding origin:%s pattern:null timestamp:null messageID:%d ~ null", opName, opName, 0);
            ctx.output(toKafka, initMsg);

            if (shouldSend) {
                String sheddingRateMap = parseMapToString(shedder.sheddingRates.map, "sheddingRatesMap", opName, "localShedding", (1000 - messageID));
                ctx.output(toKafka, sheddingRateMap);
            }
            
        } catch (Exception e) {
            aLog.error("Exception in {} for {}", opName, e);
            // Print detailed information about the objects
            aLog.error("Detailed state: shedder={}, selectivities={}, toKafka={}",  shedder.sheddingRates.toString(),
                       selectivities.toString(), 
                       toKafka);
            // Print full stack trace to stderr
            e.printStackTrace();
        }
    }
    
    /**
    * Parse method which parses a metrics value, specifically a map to a string.
    * Use this to parse messages that will be sent to our local operator.
    * It will contain the local shedding information.
    * <p>
    * @param metric the Map, we want to parse
    * @param type the type of the metric we parse
    * @param target the topic want to send the message to as the name of the traget operator
    */
   public String parseMapToString(Map<String, Double> metric, String type, String target, String pattern, int id) {
        long timestamp = System.currentTimeMillis();
        if (metric == null || metric.isEmpty()) {
            return String.format("target:%s description:%s origin:%s pattern:%s timestamp:%d messageID:%d ~ null", target, type, opName, pattern, timestamp, id);
        }
        
        StringBuilder build = new StringBuilder();
        BiConsumer<String, Double> parse = (key, value) -> {
            if (build.length() == 0) {
                build.append(String.format("target:%s description:%s origin:%s pattern:%s timestamp:%d messageID:%d ~ %s = %.6f", target, type, opName, pattern, timestamp, id, key, value));
            }
            else if (value != null) {
                // Use %.2f to show 2 decimal places for more precision
                build.append(String.format(" | %s = %.6f", key, value));
            } else {
                build.append(String.format(" | %s = null ", key));
            }
        };
        
        metric.forEach(parse);
        return build.toString();
   }


   /**
    * Parse aggregate selectivity values, into a message.
    * This message represents an SOS and should always only contain a single value
    * that get's reduced with every operator we encounter during traversal.
    * Please note, the pattern name serves 2 purposes:
    * <ul>
    *   <li> It identifies the output type and allows us to relate it to the downstream input type. This field will be updated in every operator we encoutner
    *   <li> It identifies the pattern from which the selectivity originates from. This information will not be changed and will be used to update the overloaded operator's selectivity when the final reduced selectivity will be returned.
    * </ul>  
    * @param sel specific selectivity value
    * @param target the operator we have to ultimately send the selectivity to
    * @return
    */
   public String parseSelToString(Double sel, String target, long sysTime, String outputType, String pattern, int id) {
       // we have to set the target every time anew, since we don't want to 
       // overwrite the target with every operatorname we encounter DUH
       // maybe play around with how many numbers after the digit we want to accept
       return 
           String.format("target:%s description:aggSelectivity origin:%s pattern:%s timestamp:%d messageID:%d ~ outputType:%s value:%.6f",
               target, opName, pattern, sysTime, id, outputType, sel);
   }

   /**
    * Handle messaging, when the message encounters the leaf operator
    * by redirecting the message upstream.
    * @param reducedSelectivity final selectivity to be used in the optimal shedding config
    * @param upstreamTarget the operator the message has to be sent to
    * @param relevantInputType the input that the selecitvity will be calculated over
    * @param upstreamPatternType the upstream pattern, that the selectivity will be relevant for
    * @param ctx context object for omitting to side outputs
    * @throws RuntimeException
    */
   public void handleSinkOperatorMessaging(Double reducedSelectivity, String upstreamTarget, String relevantInputType, String upstreamPatternType, Context ctx, int msgID) throws RuntimeException {
        aLog.debug(newInputRates.toString());
        aLog.info("Selectivity before integration in sink  " + opName + " for " + upstreamTarget + " is: " + reducedSelectivity + " with inputRate " + newInputRates.toString() + " and outputRates: " + outputRates.toString());
        Double result = Double.min(newInputRates.get(relevantInputType), reducedSelectivity);
        String finalSel = parseSelToString(result, upstreamTarget, System.currentTimeMillis(), "sink", upstreamPatternType, msgID);
        aLog.info("Selectivity after integration in sink " + opName + " for " + upstreamTarget + " is: " + result + " with inputRate " + newInputRates.toString() + " and outputRates: " + outputRates.toString());
        if (!upstreamTarget.isEmpty()) {
            ctx.output(toKafka, finalSel);
        }
        else {
            throw new RuntimeException("Error while reading target operator for current message.");
        }
   }

   /**
    * This method is used to calculate integrated selectivity values either
    * <ul>
    *   <li> When a downstream selectivity (either from a leaf or downstream overloaded operator) is received
    *   <li> When a message integrates it's selectivity with the result of a downstream overloaded operator
    * </ul>
    * <p>
    * With every downstream message, the local selectivity-values converge to an optimum
    * @param messageSelectivity
    * @param relevantPattern
    * @return
    */
   public Double reduceSelectivity(Double messageSelectivity, String relevantPattern) {
       Double update = null;
       //aLog.info("Trying to look for pattern: " + relevantPattern + " in " + opName);
       String msgPatternType = operator.getPattern(relevantPattern).getType();
       //aLog.info("Found pattern type " + msgPatternType + " in analyser " + opName);
       aLog.info("Selectivity before integration in " + opName + " for " + relevantPattern + " is: " + messageSelectivity + " with inputRate " + newInputRates.toString() + " and outputRates: " + outputRates.toString());
       if (msgPatternType.equals("AND") || msgPatternType.equals("SEQ")) {
            update = Double.min(messageSelectivity, selectivities.get(relevantPattern));
        } else if (msgPatternType.equals("OR")) {
            update = Double.max(messageSelectivity, selectivities.get(relevantPattern));
        }
        aLog.info("Selectivity after integration in " + opName + " for " + relevantPattern + " is: " + update + " with inputRate " + newInputRates.toString() + " and outputRates: " + outputRates.toString());
        return update;
    }
    
    /**
     * Forwards the reduced selectivities this overloaded operator receives from downstream
     * to all the operators, that subscribed to a specific pattern. This depends on
     * the subscriber's input type and to which pattern the input type leads locally.
     * <p>
     * It is possible, that operators can register with multiple different patterns 
     * and also different selectivities from the same pattern, in case the same pattern explores different subtrees for a while. 
     * </p>
     * Also maintains the list of subscribers by checking if they are outdated.
     * 
     * @param patternSubscribers
     * @param reducedSelectivity
     * @param ctx
     */
    public void forwardUpstream(String patternSubscribers, double reducedSelectivity, Context ctx, int id) 
        throws RuntimeException{
        Set<TargetMember> toRemove = new HashSet<>();
        Set<TargetMember> subscriberList = target.get(patternSubscribers);
        
        // if no operators registered, return, then we don't forward this value (right?)
        if (subscriberList == null || subscriberList.isEmpty()) {
            //aLog.info("No subscribers for pattern {}. No upstream operators currently interested in analyzer {}.", patternSubscribers, opName);
            return;
        }
        
        //aLog.info("The non-null, not empty target map in " + opName + subscriberList.toString());
        
        for (TargetMember subscriber : subscriberList) {
            double integratedSel = Math.min(reducedSelectivity, subscriber.getRegisteredSelectivity());

            if (subscriber.getTimestamp() >= traversalInitiation) {
                String integratedMsg = parseSelToString(integratedSel, subscriber.getTargetOperator(), System.currentTimeMillis(), "null", subscriber.getTargetPattern(), id);
                //aLog.info("kafka upstream forwarding message in " + opName + "analyzer, with:\n" + integratedMsg);
                ctx.output(toKafka, integratedMsg);
                aLog.info("Forwarded downstream selectivities with msg:" + integratedMsg);
                subscriber.hasBeenServed = true;
                continue;
            } else if (subscriber.hasBeenServed){
                toRemove.add(subscriber);
            } else {
                //aLog.error("Registered operator " + subscriber.operator + " didn't receive subtree selectivity from downstream operator in " + opName);
                toRemove.add(subscriber);
            }
        }
        if (!toRemove.isEmpty()) {
            subscriberList.removeAll(toRemove);
        }
    }

    /**
     * Registers an upstream Operator at the current operator to every pattern, that is
     * relevant to the input type of the message's stream.
     * 
     * I am aware of possible memory leaks in the target where an operator registers for a pattern that nobody registers at afterwards and for which we never receive a downstream message.
     * This is a small edge case tho and unlikely and would only be possible for 1 entry, since the second entry would already clean it up and any forwarding would do, too
     *  
     * @param candidates A collection of String arrays representing input mappings, with the first entry corresponding to the pattern name. This gives us all the patterns, that are relevant to a specific input type, locally
     * @param msgTarget The upstream operator, that registers itself
     * @param selPattern The pattern, that the selectivity will be relevant for
     */
    public void registerAtPattern(ArrayList<String[]> candidates, String msgTarget, String selPattern, double aggSel, Context ctx) {
        TargetMember currentMember = new TargetMember(msgTarget, selPattern, System.currentTimeMillis(), aggSel);
        Set<TargetMember> toRemove = new HashSet<>();

        for (String[] individualPattern : candidates) {
            String subscripted = individualPattern[0];
            if (!target.containsKey(subscripted)){
                Set<TargetMember> entry = new HashSet<TargetMember>();
                entry.add(currentMember);
                target.put(subscripted, entry);
            } else {
                Set<TargetMember> currentEntry = target.get(subscripted);
                if (currentEntry.size() > 25) {
                    aLog.warn("Registered upstream operators grow excessively");
                }
                for (TargetMember member : currentEntry) {
                    if (member.timestamp < traversalInitiation) {
                        toRemove.add(member);
                    }
                }

                currentEntry.add(currentMember);
                currentEntry.removeAll(toRemove);
            }
        }
    }

    public void propagateDownstream(Double aggSel, ArrayList<String[]> candidates, String msgTarget, String selPattern, String outputType, Context ctx, int id) {
        aggSel = Double.min(newInputRates.get(outputType), aggSel);

        //aLog.info("Traversing downstream");
        if (candidates.isEmpty()) {
            return;
        }
        for (String[] candidate : candidates) {
            String updatedMessage = parseSelToString(aggSel, msgTarget, System.currentTimeMillis(), candidate[0], selPattern, id);
            for (int i = 1; i < candidate.length; i++) {
                if (!tagList.containsKey(candidate[i])) {
                    throw new RuntimeException("Mapping between input and output or output tag list  wrong ");
                } else {
                    ctx.output(tagList.get(candidate[i]), updatedMessage);
                    messageID += 1;
                }
            }
        }
    }

    /**
     * Process string type messages from
     * <ul>
     * <li> Operator's Kafka topic (mix between messages we just wrote to kafka so that the shedder can fetch them and downstream operators wrote)
     * <li> Upstream operators in form of SOS messages
     * </ul>
     * <p>
     * Messages that have the same origin as the operator.name are not relevant to the analyzer, but the shedder
     * <p>
     * A selectivity/sos message has the form of: 
     * "target:%s description:selectivity origin:%s timestamp:%d ~ outputTape:%s value:%.6f"
     * 
     * @param msg The actualy message
     * @param ctx A context object we specifically use to emit to side Outputs
     * @param out The main output
     */
    @Override
    public void processElement2(String msg, Context ctx, Collector<String> out) {
        // unpack the selectivities
        // integrate them into the new selectivity collection
        // check if we are overloaded (yes -> wait for selectivites, coordinate and integrate information)
        // check if we are overloaded and notified the shedder (-> wait and then forward)
        

        // can we avoid the parsing in any wayy?
        String[] segments = msg.split(Pattern.quote("~"));
        String header = segments[0];
        String mapToken = segments[1];
        
        //parse the header to check if we are processing the correct message 
        Matcher matchHeader = headerPattern.matcher(header);
        
        String msgTarget = "default";
        String desc = "default";
        String origin = "default";
        String selPattern = "default";
        long timestamp = 0;
        int msgID = 0;
        
        // decode header
        if (matchHeader.find()) {
            msgTarget = matchHeader.group(1);
            desc = matchHeader.group(2);
            origin = matchHeader.group(3);
            selPattern = matchHeader.group(4);
            timestamp = Long.parseLong(matchHeader.group(5));
            msgID = Integer.parseInt(matchHeader.group(6));
        }
        
        boolean selStillValid = timestamp - (selectivities.get("birth") != null ? selectivities.get("birth").longValue() : 0) <= lifeTime;
        
        // now unnessesary overhead -> can compile the whole pattern once
        Matcher matcher = regPattern.matcher(mapToken);
        double aggSel = Double.POSITIVE_INFINITY;
        String outputType = "";
        
        // can maybe store the timediffs, calculate the average
        // on some messages and use this as an updated dynamic timeout
        // there are inconsistencies here
        long timeDiff = traversalInitiation > 0 ? timestamp - traversalInitiation : -1;
        if (!operator.isSinkOperator){
            aLog.info(opName + " timestamp = " + timestamp + ", traversalInitiation = " + traversalInitiation + "for msgID: " + msgID);
            aLog.info(String.format("Time passed since traversal of %s was initiated: %d ", opName, timeDiff));
        }

        while (matcher.find()) {
            outputType = matcher.group(1);
            aggSel = matcher.group(2) == "Infinity" ? Double.POSITIVE_INFINITY : Double.valueOf(matcher.group(2));
        }

        
        ArrayList<String[]> candidates = inputToOutput.get(outputType); 
        // 1a. We are NOT the target operator of the message, thus receive an upstream message
        if (!msgTarget.equals(opName)) {
            aLog.info("Selectivities from upstream in " + opName + "for input " + outputType + " are " + aggSel + ". With the send message:" + msg);
            Double updatedAggSel = Double.min(newInputRates.get(outputType), aggSel);

            // if we are the sink and recv upstream msg, forward final sels back to target operator from the message
            if (operator.isSinkOperator) {
                handleSinkOperatorMessaging(aggSel, msgTarget, outputType, selPattern, ctx, messageID);
                return;
            }

            // 2a. We ARE overloaded, thus already collecting, not a sink and thus register
            if (isOverloaded) {
                aLog.info("Register at " + opName);
                // only register, if the msg arrived in the traversal timeframe
                // 3 s timeout for now

                if (timeDiff < timeout) {
                    // we know where to send, but which selectivity do we send to whom?
                    // for the amount of time we collect all the selectivities we receive
                    // but have to forward them corrispondingly
                    if (!candidates.isEmpty()) {
                        registerAtPattern(candidates, msgTarget, selPattern, aggSel, ctx);
                    }
                    return;
                } 
                // in case we timed out, it is likely, that immediately traversing is
                // unneccesary overhead and we can still use the latest selectivity to send upstream immediately (tradeoff with accuracy)
                // but we have to be not overloaded and not collecting. Collecting and overload are syncrhonized thus the check
                
                else if (selStillValid && !collecting) {
                    // we shouldn be collecting in the first place
                    //collecting = false;
                    gradualChanges = 0.;
                    //aLog.info("Encountered after timeout in " + opName + "but selectivities are still valid, thus try forwarding upstream.");

                    if (candidates.isEmpty()) {
                        throw new RuntimeException("Registration at downstream operator failed");
                    }
                    for (String[] candidate : candidates) {
                        try {
                            //String localPatternType = operator.getPattern(candidate[0]).getType();
                            //aLog.info("Found pattern type " + localPatternType + " in analyser " + opName);
                            aLog.info("We are not the taget as " +  opName + "but still need to integrate the selectivity.");
                            Double reducedSel = reduceSelectivity(aggSel, candidate[0]);
                            //aLog.info("Reduced selectivitiy in " + opName);
                            String upstream = parseSelToString(reducedSel, msgTarget, System.currentTimeMillis(), "null", selPattern, msgID);
                            
                            // the msgTarget will be used to forward to the correct topic
                            ctx.output(toKafka, upstream);
                            return;
                        } catch (Exception e) {
                            aLog.debug("Exception while trying to forward timed out but alive selectivities:" + e);
                            throw new RuntimeException("Can access pattern with " + candidate[0] + e + " for mappings " + inputToOutput.toString());
                        }
                    }
                }
                else if (!selStillValid) {
                    //clear selectivities and targets when the selectivities die
                    selectivities.clear();
                    //target.clear();

                    gradualChanges = 0.;
                    propagateDownstream(updatedAggSel, candidates, msgTarget, selPattern, outputType, ctx, msgID);
                    return;
                }

                // now stop this traversal
                
                // 2b. We are NOT overloaded
            } else {
                // propagate the message downstream
                propagateDownstream(updatedAggSel, candidates, msgTarget, selPattern, outputType, ctx, msgID);
                return;
            }
        }

        // 1b. We ARE the target operator, thus receive a downstream selectivity (only when we initiated the traversal prior to this)
        else {
            aLog.info("Selectivities from downstream in " + opName + "for input " + outputType + " are " + aggSel + ". With the send message:" + msg);
            if (operator.isSinkOperator) {
                throw new IllegalArgumentException("The sink operator should not receive downstream messages");
            }
            Double update = reduceSelectivity(aggSel, selPattern);

            // 3a. We timed out but the selectivity still lives
            if (timeDiff > timeout && selStillValid) {
                // if the timestamp exceeds the 3s timeframe but arrives in the sel maps lifetime
                // still update the map, but don't forward the message anymore
                // this is just for traversals that appear shortly after the timeout and nobody else
                
                collecting = false;
                selectivities.put(selPattern, update);
                gradualChanges = 0.;
                return;

            // 3b. We didn't time out thus can update the local selectivities and forward the refined selectivity to the shedder and logged waiting operators
            // I think we are always collecting in this case
            } else if (timeDiff <= timeout && collecting) {
                double oldSelectivities = selectivities.get(selPattern);
                selectivities.put(selPattern, update);

                boolean granularChange = Math.abs(oldSelectivities - update) > 0.05 * oldSelectivities;
                gradualChanges += Math.abs(oldSelectivities - update) / oldSelectivities;
                boolean sigChange = gradualChanges > 0.07;
                
                // if consecutive selectivities change by 5% or we reach an overall selectivity change of 7%, we update the
                // shedding rate of the specific pattern and send them to the operator
                if (granularChange || sigChange) {
                    Map<String,Double> patternSpecificRates = shedder.calculateSheddingRates(selPattern, this.newInputRates, this.outputRates, this.processingTimes, this.selectivities);
                    
                    String updatedRates = parseMapToString(patternSpecificRates, "sheddingRatesMap", opName, selPattern, msgID);
                    if (sigChange) {
                        gradualChanges = 0.;
                    }
                    ctx.output(toKafka, updatedRates);
                }

                forwardUpstream(selPattern, update, ctx, msgID);

                return;
            }
        }
        
        // maybe move this into processElement1 method so that we ensure consistencies with both channels
        long timeSpendCollecting = System.currentTimeMillis() - traversalInitiation;
        if (timeSpendCollecting > timeout) {
            collecting = false;
        }
    }
}   
