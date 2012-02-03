;; Main functions to connect to the PSMSL Oracle database
;; and produces OceanSites 1.2 compliant netcdf files of
;; monthly mean sea levels from RLR stations

;; Run: (-main)

(ns org.psmsl.netcdf.core
  (:use [korma.db]
        [korma.core]
        [org.psmsl.netcdf.boilerplate]
        [org.psmsl.netcdf.config]
        [clj-time.core]
        [clj-time.format])
  (:refer-clojure :exclude [extend])
  (:import
   (ucar.ma2 DataType ArrayFloat ArrayDouble ArrayShort InvalidRangeException)
   (ucar.nc2 NetcdfFileWriteable Dimension)
   (java.util ArrayList)
   (java.io IOException)
   (org.joda.time ReadableDateTime DateTime DateMidnight DateTimeZone
                  Period PeriodType Interval LocalDate Days)
   (org.joda.time.format DateTimeFormatter DateTimeFormat))
  (:require [clojure.java.io :as io])
  (:gen-class))

;; Database connection
(defdb psmsldb {:classname "oracle.jdbc.driver.OracleDriver"
                :subprotocol "oracle:thin"
                ;; Database connection details are read from org.psmsl.netcdf.config
                :subname dbname
                ;;fixes the quoting of strings to oracle
                :delimiters ""
                :user user
                :password pass
                ;;set naming strategy - makes uppercase oracle fields be
                ;;referenced by lowercase clojure fields
                :naming {:keys clojure.string/lower-case
                         :fields clojure.string/upper-case}})

;; Set up the entities for tables and relationships of interest
;; Tables we are interested in:
(defentity monthly)
(defentity country)
(defentity address)

;; Set a global var for missing data values
(def missval -9999.0)

(defn clob-to-string [clob]
  "Turn an Oracle Clob into a String"
  (with-open [rdr (java.io.BufferedReader. (.getCharacterStream clob))]
    (apply str (line-seq rdr))))

;; Sets up relationship - each station is only referred to once in monthly
(defentity station
;; Makes sure that station.id gets joined to monthly.id
  (has-one monthly {:fk :id})
;; Makes sure that station.country gets joined to country.id
  (has-one country {:fk :id}))

(defentity stationmeta
  (table :station)
;; Makes sure that station.id gets joined to monthly.id
  (has-one monthly {:fk :id})
;; Makes sure that station.country gets joined to country.id
  (has-one country {:fk :id})
  (transform #(-> %
                  (update-in [:documentation] clob-to-string))))
  
(defn make-single-coord [val]
  "Returns a single value as a co-ordinate"
    (ArrayFloat/factory 
      (float-array 1 (float val))))

(defn data-file [filename]
  (NetcdfFileWriteable. filename false))

(defn make-double-array [n]
  "Returns a 1xn Java array of doubles"
   (ArrayDouble/factory (double-array n)))

(defn make-short-array [n]
  "Returns a 1xn Java array of shorts"
  (ArrayShort/factory (short-array n)))

(defn make-shorts [list]
  "Returns list of shorts from list of non-shorts"
  (map #(short %) list))

(defn days-elapsed [date]
  "Returns the days elapsed since 0000-01-01T00:00:00Z until the given date"
  (.getDays (Days/daysBetween (DateTime. 0 1 1 0 0 0) date)))

(defn make-file-name [name]
  "Returns a path to a file in the current directory"
  (.getCanonicalPath (io/file (System/getProperty "user.dir")
                              (str "PSMSL_" name ".nc"))))
(defn make-seq [argmap & key]
  "Produces flattened vector from the arraymap returned from the database.
   If a named key is passed then a flattened map containing that key will be
   returned."
  ;; Should check whether passed key exists in map
  (let [numkeys (count key)
        firstkey (first key)]
    (cond 
     (= numkeys 0) (flatten (map #(vals (argmap %))
                                 (range 0 (count argmap))))
     (= numkeys 1) (flatten (map #((argmap %) firstkey)
                                 (range 0 (count argmap))))
     :else  (do
        (println "WARNING: only first key will be returned in: make-seq")
        (flatten (map #((argmap %) firstkey) (range 0 (count argmap))))))))

(defn make-dates [dates]
  "Takes a list of Date objects from Oracle and converts to Joda DateTimes"
  (map #(.toDateTimeAtStartOfDay (LocalDate. %)) dates))

(defn elapsed-days-since-zero [datetimes]
  "Takes a list of DateTime objects and returns a list of the number of
   elapsed days since 1/1/00"
  (map #(days-elapsed %) datetimes))

(defn make-times [res]
  "Return a Java double array of elapsed times since 1/1/00
  from an Oracle results map"
  (make-double-array (elapsed-days-since-zero
                      (make-dates (map :time res)))))

(defn replace-nils [data]
  "Replaces nils within a list with a missing value, missval"
  (map #(cond (= % nil) missval :else %) data))

(defn make-heights [res]
  "Return a Java short array of sea level heights from an Oracle
   results map"
  (make-short-array (make-shorts (replace-nils (map :rlrdata res)))))

(defn unmap [mapname keyname]
  "Returns a list of the given keys extracted from the map"
  (#(first (map % mapname)) keyname))

(defn id-str [id]
  "Returns a formatted string for the netcdf file of the form
   'OS_PSMSL-FDM057_20110912_R'"
  (let [oformat (DateTimeFormat/forPattern "yyyyMMdd")]
    (str "OS_PSMSL-FDM" (format "%04d" (.intValue id))
       "_" (. oformat print (DateTime.)) "_R")))

(defn write-data [data metadata datafile]
  (let [;; Create the coordinate data
        lat (make-single-coord (unmap metadata :latitude))
        lon (make-single-coord (unmap metadata :longitude))
        comment (unmap metadata :documentation)
        name (unmap metadata :name)
        psmslid (unmap metadata :id)
        glossid (unmap metadata :glossid)
        country (unmap metadata :country)
        country2 (unmap metadata :country2)
        supplier (unmap metadata :supplier)
        ;; Formatted id string a la UHSLC
        idstr (id-str psmslid)
        ;; Create the times in days since 1/1/00
        time (make-times data)
        heights (make-heights data)]
    (doto datafile
      ;; Add global attributes from metadata
      (.addGlobalAttribute "comment" comment)
      (.addGlobalAttribute "station_name" name)
      (.addGlobalAttribute "PSMSL_ID" (str psmslid))
      (.addGlobalAttribute "GLOSS_ID" (str glossid))
      (.addGlobalAttribute "country" country)
      (.addGlobalAttribute "iso3166-2" country2)
      (.addGlobalAttribute "contributor_name" supplier)
      (.addGlobalAttribute "id" idstr)
      (.addGlobalAttribute "missing_value" missval)
      
    ;; Write the coordinate variable data. This will put the latitudes
    ;; and longitudes of our data grid into the netCDF file.
      (.create)

      ;; Actually write the data to the file
      (.write "latitude" lat)
      (.write "longitude" lon)
      (.write "time" time)
      (.write "sea_surface_height_above_reference_level" heights)
      (.close))
    (str psmslid)))
  
(defn get-monthly-rlr-data [station_id]
  "Get the monthly RLR data and dates for a given id"
  (let [res (select station
                    (with monthly)
                    (fields :station.name :monthly.time :monthly.rlrdata)
                 (where (= :id station_id))
                 (order :monthly.time :ASC))]
    res))

(defn get-station-metadata [station_id]
  "Get the station metadata for a given id"
  (let [res (transaction (select stationmeta
                    (join :country (= :station.country :country.id))
                    (join :address (= :station.supplier :address.id))
                    (fields :station.name :station.glossid :station.latitude
                            :station.longitude
                            [:address.name :supplier]
                            [:country.name :country]
                            [:country.text2 :country2]
                            :station.documentation
                            :station.id)
                 (where (= :id station_id))
                 (order :station.name :ASC)))]
    res))

(defn get-rlr-ids []
  "Get the IDs of all stations with RLR data"
  (let [res (select station
                    (fields :id)
                    (where (= :isrlr "Y"))
                    (order :id))]
    res))

(defn make-file [datafile id]
  "Adds data to a netcdf datafile"
  (let [data (get-monthly-rlr-data id)
        meta-data (get-station-metadata id)]
    (write-data data meta-data datafile)))

(defn make-rlr-netcdf [ids]
  "Takes a sequence of ids and produces netcdf files from their data"
  (for [id (take 10 ids)]
    ;; Add boilerplate fields to the datafile
    (let [fname (make-file-name (format "%04d" (.intValue id)))]
      (make-file (boilerplate (data-file fname)) id))))
  
(defn -main []
  "Run the main program"
  ;; Make a netcdf file for each station
  (let [station-ids (get-rlr-ids)
        ids (map :id station-ids)]
    (make-rlr-netcdf ids)))


