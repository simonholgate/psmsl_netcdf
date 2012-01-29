(ns org.psmsl.netcdf.boilerplate
  (:import
   (ucar.ma2 DataType ArrayFloat InvalidRangeException)
   (ucar.nc2 NetcdfFileWriteable Dimension)
   (java.util ArrayList)
   (java.io IOException))
  (:gen-class))

(defn boilerplate [datafile]
  "Adds boilerplate features to the netcdf file"
;;   dimensions:
;; 	time = UNLIMITED ; // (1284 currently)
;; 	depth = 1 ;
;; 	latitude = 1 ;
;; 	longitude = 1 ;
;; variables:
;; 	double time(time) ;
;; 		time:standard_name = "time" ;
;; 		time:long_name = "time" ;
;; 		time:units = "days since 0000-01-01T00:00:00Z" ;
;; 		time:calendar = "gregorian" ;
;; 	float depth(depth) ;
;; 		depth:standard_name = "depth" ;
;; 		depth:long_name = "depth" ;
;; 		depth:units = "m" ;
;; 	float latitude(latitude) ;
;; 		latitude:standard_name = "latitude" ;
;; 		latitude:long_name = "latitude" ;
;; 		latitude:units = "degrees_north" ;
;; 	float longitude(longitude) ;
;; 		longitude:standard_name = "longitude" ;
;; 		longitude:long_name = "longitude" ;
;; 		longitude:units = "degrees_west" ;
;; 	short sea_surface_height_above_reference_level(time, depth, latitude, longitude) ;
;; 		sea_surface_height_above_reference_level:long_name = "Sea Level (MONTHLY)" ;
;; 		sea_surface_height_above_reference_level:units = "millimeters" ;
;; 		sea_surface_height_above_reference_level:_FillValue = -32768s ;
;; 		sea_surface_height_above_reference_level:ancillary_variables = "sensor_type_code" ;
;; 	byte sensor_type_code(time, depth, latitude, longitude) ;
;; 		sensor_type_code:standard_name = "sensor_type_code status_flag" ;
;; 		sensor_type_code:long_name = "sensor type code" ;
;; 		sensor_type_code:_FillValue = 0b ;
;; 		sensor_type_code:valid_range = 1b, 4b ;
;; 		sensor_type_code:flag_values = 1b, 2b, 3b, 4b ;
;; 		sensor_type_code:flag_masks = 7b, 7b, 7b, 7b ;
;; 		sensor_type_code:flag_meanings = "unknown float_gauge echo_sounder radar" ;

;; // global attributes:
;; 		:Conventions = "CF-1.5, OceanSITES 1.2, TideGauge-0.1" ;
;; 		:title = "Sea Level Time Series (MONTHLY)" ;
;; 		:naming_authority = "OceanSITES" ;
  ;; 		:id = "OS_UH-FDM057_20110912_R" ;
  (let [lat-dim (.addDimension datafile "latitude" 1)
        lon-dim (.addDimension datafile "longitude" 1)
        depth-dim (.addDimension datafile "depth" 1)
        time-dim (.addUnlimitedDimension datafile "time") ]
     (doto datafile
      (.addVariable "latitude" (DataType/FLOAT) 
                    (into-array Dimension [lat-dim]))
      (.addVariable "longitude" (DataType/FLOAT) 
                    (into-array Dimension [lon-dim]))
      (.addVariable "depth" (DataType/FLOAT) 
                    (into-array Dimension [depth-dim]))
      (.addVariable "time" (DataType/DOUBLE) 
                    (into-array Dimension [time-dim]))
      (.addVariable "sea_surface_height_above_reference_level" (DataType/SHORT) 
                    (into-array Dimension [time-dim]))
      ;;
      (.addVariableAttribute "longitude" "standard_name" "longitude")
      (.addVariableAttribute "longitude" "long_name" "longitude")
      (.addVariableAttribute "longitude" "units" "degrees_east")
      ;;
      (.addVariableAttribute "latitude" "standard_name" "latitude")
      (.addVariableAttribute "latitude" "long_name" "latitude")
      (.addVariableAttribute "latitude" "units" "degrees_north")
      ;;
      (.addVariableAttribute "depth" "standard_name" "depth")
      (.addVariableAttribute "depth" "long_name" "depth")
      (.addVariableAttribute "depth" "units" "mm")
      ;;
      (.addVariableAttribute "time" "standard_name" "time")
      (.addVariableAttribute "time" "long_name" "time")
      (.addVariableAttribute "time" "units" "days since 0000-01-01T00:00:00Z")
      (.addVariableAttribute "time" "calendar" "gregorian"))
    datafile))                          