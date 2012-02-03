(defproject psmsl_netcdf "0.0.1-SNAPSHOT"
            :description "Writing individual netcdf files for stations retrieved from the PSMSL database"
            :main org.psmsl.netcdf.core
            :dependencies [[org.clojure/clojure "1.3.0"]
                           [netcdf/netcdfAll "4.2"]
                           [korma "0.3.0-beta1"]
                           [oracleJDBC/classes12 "10.2.0"]
                           [clj-time "0.3.4"]])