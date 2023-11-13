This is the codebase for the master thesis "Live Adaption of Privacy-Enhancing-Technologies in the Connected Vehicles' Datapipelines.

*** Mapping of packages to the components in architecture ***

The input sources are located in "thesis.common.sources". They implement the SourceFunction, so that the data are generated directly programmtically inside these classes. If external sources are desired, such as Kafka producer, in "thesis.common.producer" several Kafka producers can be found. The corresponding Serializer and Deserializer (serdes) are under "thsis.common.serdes". For the usage of these, please refer to the documentation of Kafka and other online resources. Due to the fact that the sources in proof-of-concept are Flink's SourceFunctions, the Kafka producer and serdes are deprecated and not maintained. They are uploaded to provide a start for the succeeding researcher.

The classes implementing the input data can be found in the following packages: Vehicle Context - "thesis.context", Policy - "thesis.policy". The underlying sensor readings can be found in "thesis.context.data".

The Situation Evaluator lies in "thesis.flink.SituationEvaluatorFromContext".

In "thesis.flink", most of the operators can be found, such as the "ControlledBuffer" and "Feedback Processor" in distributed PET enforcement.

The Data Sinks are in "thesis.flink.sink". The DataLogger is the sink for producing measurement results for the evaluation. The ImageSink displays images. The UI and relevant components for demonstration are in "thesis.demo"

*** How to create / start a streaming job ***

Evaluation jobs and demo jobs are in the package thesis.jobs. Currently, to build a new pipeline, the code should be in most cases hand-written. Due to the time restriction, the aspect of user friendliness of constructing pipelines is underprioritized. Pipelines involving single data section can be found among the evaluation jobs. For pipeline involving multiple data sections, please refer to the job with the name "Demo".