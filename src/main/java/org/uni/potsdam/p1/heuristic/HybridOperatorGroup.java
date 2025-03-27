package org.uni.potsdam.p1.heuristic;

import java.util.Map;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.uni.potsdam.p1.actors.operators.groups.AbstractOperatorGroup;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.EventOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;
import org.uni.potsdam.p1.types.outputTags.StringOutput;
import org.uni.potsdam.p1.variant.VariantSourceCounter;

/**
 * Operator group for hybrid load shedding setup.
 * Contains a single operator that handles its own statistics, analysis,
 * and communication with the coordinator.
 */
public class HybridOperatorGroup extends AbstractOperatorGroup {
    //Need side outputs for:
    // shedder -> thisOperator
    // thisOperator -> analyzer
    // thisOperator -> downstreamShedder
    // thisOperator -> downStreamAnalyzer

    public final MetricsOutput toThisAnalyser;
    public final MetricsOutput downstreamAnalyser;
    public final EventOutput downstream;
    public StringOutput produceToKafka;
    public StringOutput kafkaToAnalyser;
    public StringOutput kafkaToOperator;
    
    // components of the operator group
    private HybridOperator operator;
    public HybridAnalyser analyser;
    public HybridMessenger messenger;

    public SingleOutputStreamOperator<String> messengerStream;
    public DataStream<Event> operatorStream;
    // upstream input from analyzer
    public DataStream<Metrics> analyzerInputStream;
    // upstream sos from analyzers
    public SingleOutputStreamOperator<String> analyserOutputStream; 
    // global KafkaProducer Stream
    public KafkaSink<String> globalChannelOut;

    public HybridOperatorGroup(OperatorInfo operatorInfo) {
        super(operatorInfo);

        operator = new HybridOperator(operatorInfo);
        analyser = new HybridAnalyser(operatorInfo);
        
        
        kafkaToAnalyser = new StringOutput("to_analyser" + operatorInfo.name);
        kafkaToOperator = new StringOutput("to_shedder" + operatorInfo.name);
        toThisAnalyser = new MetricsOutput("to_analyser_" + operatorInfo.name);
        downstream = new EventOutput("downstream" + operatorInfo.name);  
        downstreamAnalyser = new MetricsOutput("downstream" + operatorInfo.name);  
        
        messenger = new HybridMessenger(operatorInfo, kafkaToAnalyser, kafkaToOperator);
        
        // we only need outputRate and processingTimes for shedding calculations
        operator.setMetricsOutput("labmdaOut", toThisAnalyser);
        operator.setMetricsOutput("ptime", toThisAnalyser);
        operator.setMetricsOutput("lambdaOut", downstreamAnalyser);
    }

    /**
     * This should be the rough group architecture:
     *  input --> shedder --> operator --> output
     *              |           | 
     *              |        analyzer --> traversal
     *              |           |
     *                  ||
     *                Kafka        
     *  
     */
    public void createDataStream(DataStream<String> opKafkaStream) {
        String opName = operatorInfo.name;
    
        if (opKafkaStream == null) {
            System.out.println("Warning: Kafka stream for operator " + opName + " is null");
            return;
        }
    
        messengerStream = opKafkaStream
            .process(messenger)
            .name("messenger" + opName);
        
        if (inputDataStreams == null) {
            System.out.println("Warning: Input stream for operator " + opName + " is null");
            return;
        }
        
        // Main processing stream to shedder
        SingleOutputStreamOperator<Event> outputDataStream = inputDataStreams
            .connect(messengerStream.getSideOutput(kafkaToOperator))
            .process(operator)
            .name("to_operator"+ opName);
        
        this.outputDataStream = outputDataStream;
        
        // Initialize streams if they're null
        DataStream<Metrics> metricsStream = outputDataStream.getSideOutput(toThisAnalyser);
        if (analyzerInputStream == null) {
            analyzerInputStream = metricsStream;
        } else {
            analyzerInputStream = analyzerInputStream.union(metricsStream);
        }
        
        DataStream<String> kafkaMessages = messengerStream.getSideOutput(kafkaToAnalyser);
        
        // Only create the analyzer output stream if we have both required inputs
        if (analyzerInputStream != null && (analyserOutputStream != null || kafkaMessages != null)) {
            DataStream<String> connectStream = analyserOutputStream != null ? 
                (kafkaMessages != null ? analyserOutputStream.union(kafkaMessages) : analyserOutputStream) :
                kafkaMessages;
                
            SingleOutputStreamOperator<String> analyzerOutpuStream = analyzerInputStream
                .connect(connectStream)
                .process(analyser)
                .name("analyser" + opName);
    
            this.analyserOutputStream = analyzerOutpuStream;
            
            if (produceToKafka != null && globalChannelOut != null) {
                analyzerOutpuStream.getSideOutput(produceToKafka).sinkTo(globalChannelOut);
            }
        }
        
        if (operatorInfo.executionGroup != null) {
            outputDataStream.slotSharingGroup(operatorInfo.executionGroup);
            if (analyserOutputStream != null) {
                analyserOutputStream.slotSharingGroup(operatorInfo.executionGroup);
            }
            messengerStream.slotSharingGroup(operatorInfo.executionGroup);
        }
    }

    @Override
    // Sets output for the operators
    public void setOutputs(Map<String, AbstractOperatorGroup> operatorGroupMap) {
        for (EventPattern pattern : operatorInfo.patterns) {
            for (String operatorName : pattern.downstreamOperators) {
                EventOutput currentComplexEventOutput = operatorGroupMap.get(operatorName).toThisOperator;
                operator.setSideOutput(operatorName, currentComplexEventOutput);
                HybridOperatorGroup dsOp = (HybridOperatorGroup) operatorGroupMap.get(operatorName);
                operator.setMetricsOutput("lambdaOut", dsOp.toThisAnalyser);
            }
        }
    }

    public void addInputStream(DataStream<Event> inputStream) {
        if (inputStream == null) {
            return;
        }

        if (this.inputDataStreams == null) {
            this.inputDataStreams = inputStream;
        } else {
            inputDataStreams = inputDataStreams.union(inputStream);
        }

        DataStream<Metrics> sourceOutputRates = inputStream
            .process(new VariantSourceCounter(operatorInfo))
            .name("Source Counter");

        if (analyzerInputStream == null) {
            analyzerInputStream = sourceOutputRates;
        } else {
            analyzerInputStream = analyzerInputStream.union(sourceOutputRates);
        }
    }

    // set metrics stream for inputRates to downstream analyzer
    public void addAnalyzerInputStream(SingleOutputStreamOperator<Event> inputRates) {
        if (inputRates == null) {
            return;
        }

        DataStream<Metrics> stream = inputRates.getSideOutput(toThisAnalyser);
        if (stream == null) {
            return;
        }

        if (analyzerInputStream == null) {
            analyzerInputStream = stream;
        } else {
            analyzerInputStream.union(stream);
        }
    }

    // set sosStream for selectivities to downstream analyzer
    public void addAnalyserSOSStream(SingleOutputStreamOperator<String> sosStream) {
        if (sosStream == null) {
            return;
        }

        if (analyserOutputStream == null) {
            analyserOutputStream = sosStream;
        }
        
        else {
            analyserOutputStream.union(sosStream);
        }
    }

    //set up the outgoing kafka connection the analyzer uses
    public HybridOperatorGroup connectToKafka(StringOutput produceToKafka,KafkaSink<String> globalChannelOut) {
        this.produceToKafka = produceToKafka;
        this.globalChannelOut = globalChannelOut;
        return this;
    }

}