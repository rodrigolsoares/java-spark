package com.sparkTutorial.rdd.airports;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.sparkTutorial.rdd.commons.Utils;

/* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
and output the airport's name and the city's name to out/airports_in_usa.text.

Each row of the input file contains the following columns:
Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

Sample output:
"Putnam County Airport", "Greencastle"
"Dowagiac Municipal Airport", "Dowagiac"
...
*/

public class AirportsInUsaProblem {

    public static void main(String[] args) throws Exception {

    	 //Configurando o Spark
    	 Logger.getLogger("org").setLevel(Level.ERROR);
         SparkConf conf = new SparkConf().setAppName("airportsInUsaProblem").setMaster("local[1]");
         JavaSparkContext sc = new JavaSparkContext(conf);
         
         //Carregando o arquivo
         JavaRDD<String> lines = sc.textFile("in/airports.text");
         
         //Filtro, pegando todos os registros que o país é United States
         JavaRDD<String> airportsUsa = lines.filter(filterCountry());
         
         //Criando o layout, do arquivos
         JavaRDD<String> airportsNameAndCityNames = airportsUsa.map(filterAiports());
         
         //Criando o arquivo de saída com os registros
         airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text");
         
    }

	private static Function<String, Boolean> filterCountry() {
		return line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\"");
	}

	private static Function<String, String> filterAiports() {
		return line -> {return montaArquivos(line);};
	}

	private static String montaArquivos(String line) {
		String[] splits = line.split(Utils.COMMA_DELIMITER);
		return StringUtils.join(new String[]{splits[1], splits[2]}, ",");
	}
}

