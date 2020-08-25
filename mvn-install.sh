# 1. Run the following command
mvn clean install -DskipTests
mvn install:install-file -Dfile=./target/materialiser-0.2.0.jar -DgroupId=upm.oeg.helio -DartifactId=materialiser -Dversion=0.2.0 -Dpackaging=jar

# 2. In the pom of your project import the follwing dependencies
#
#	<!-- Helio materialiser -->
#	  	<dependency>
#			<groupId>upm.oeg.helio</groupId>
#		   	<artifactId>materialiser</artifactId>
#		    <version>0.2.0</version>
#		</dependency>
#		