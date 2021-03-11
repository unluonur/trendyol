name := "trendyol"

version := "0.1"

scalaVersion := "2.12.2"

val flinkVersion = "1.12.2"

libraryDependencies ++= Seq(
  "org.apache.flink"      %% "flink-scala"            % flinkVersion,
  "org.apache.flink"      %% "flink-streaming-scala"  % flinkVersion,
  "org.apache.flink"      %% "flink-runtime-web"      % flinkVersion,
  "org.apache.flink"      %% "flink-test-utils"       % flinkVersion    % "test"
)
