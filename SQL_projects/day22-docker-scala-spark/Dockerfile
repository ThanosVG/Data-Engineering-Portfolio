FROM eclipse-temurin:17-jdk

WORKDIR /app

# Install Scala and sbt
RUN apt-get update && apt-get install -y wget gnupg
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add
RUN apt-get update && apt-get install -y scala sbt && rm -rf /var/lib/apt/lists/*

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar xvf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Copy Scala script and data
COPY Day15Etl.scala .
COPY salaries.csv .
COPY departments.csv .

# Build with sbt (add assembly plugin)
RUN mkdir -p project && echo 'addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.0.0")' > project/plugins.sbt
RUN echo 'name := "Day15Etl"\nversion := "0.1"\nscalaVersion := "2.12.18"\nmainClass in assembly := Some("Day15Etl")\nlibraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "3.5.0" % "provided")' > build.sbt
RUN sbt assembly

# Run JAR
CMD ["spark-submit", "--class", "Day15Etl", "target/scala-2.12/Day15Etl-assembly-0.1.jar"]