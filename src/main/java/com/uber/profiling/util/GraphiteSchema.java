package com.uber.profiling.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class GraphiteSchema {
    public static void main(String[] args) throws FileNotFoundException { get_schema();}
        public static String get_schema() throws FileNotFoundException  {
            String oneops_variable = "ONEOPS_NSPATH";
            String org;
            String schema = "";
            String assembly;
            String platform;
            String env;
            String graphite_schema;

//            Scanner scanner = new Scanner(new File("/app/graphite_schema")); // path to file
            Scanner scanner = new Scanner(new File("/etc/oneops")); // path to file

            while (scanner.hasNextLine()) {
//                schema = scanner.nextLine();
                String line = scanner.nextLine();
                if (line.contains(oneops_variable)) { // check if line has your finding word
                    schema = line.split("=")[1];
                }
            }

            org = schema.split("/")[1];
            assembly = schema.split("/")[2];
            platform = schema.split("/")[5];
            env = schema.split("/")[3];

            graphite_schema = org+"."+assembly+"."+env+"."+platform;
            return graphite_schema;
//            return schema;
    }
}