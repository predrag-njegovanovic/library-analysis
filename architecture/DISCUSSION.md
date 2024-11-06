# Discussion about application design

The main misson of the task was to analyze the library data, build a model and answer some business questions. This was more of a Data Science type of work and since I applied to Data Engineer role, the architecture design of the application follows some "modern" practices in the developlment of the machine learning models where I want to involve more data engineering work.

The main idea is to have a data pipeline refining the data until it hits satisfactory level where it is ready to be used for different use cases, one of them being ML training. This is similar to modern data platform (like Databricks) advocating medallion architecture and separating data storage and processing.
Data scientists would analyze the data in the silver layer, come up with the right features and then we can create the dataset in the gold layers, as well as the feature store. This is continuous process.

Since the workload is small, Polars is chosen as a data processing engine. It has very similar dataframe API as the Spark but the code is written to be extendible so PySpark can be used as well.

Data is placed in the three layers locally (this would be cloud object storage otherwise) and the `parquet` format is used in all three layers. It has schema, it allows for compression but it lacks in other departments (no partial updates for example).
Silver and gold layer can be extended with open table formats (Delta or Iceberg) but no complication is needed for this type of task.

The trained model is simple and the ML process needs to be expanded to support full production use (e.g. experiment tracking, model versioning, retraining orchestration etc.). `Predict` command simulates the model serving.

This task should represent the "modern data platform" in a nutshell. The task could have been implemented through couple of notebooks with ad-hoc preprocessing and cleaning but that approach is not testable.

*DISCLAIMER*: I haven't managed to write tests for all applications. There needs to be more unit test for transformations and aggregation functions. However, I've shown the principle of how to write a test using `pytest` and `polars.testing`. During testing I've discovered that `clean_dates` function is not behaving as expected and some functions need refactoring.

Unfortunately, I've had conference to attend so I couldn't use all given days for this task. However, the principles and concepts are demonstrated on how to continue developing this kind of a project.
