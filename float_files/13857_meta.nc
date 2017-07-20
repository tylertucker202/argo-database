CDF       
   
   	DATE_TIME         STRING2       STRING4       STRING8       STRING16      STRING32       STRING64   @   	STRING256         N_CYCLES      N_PARAM                  5   	DATA_TYPE                  comment       	Data type      
_FillValue                    "\   FORMAT_VERSION                 comment       File format version    
_FillValue                    "l   HANDBOOK_VERSION               comment       Data handbook version      
_FillValue                    "p   DATE_CREATION                   comment       Date of file creation      conventions       YYYYMMDDHHMISS     
_FillValue                    "t   DATE_UPDATE                 	long_name         Date of update of this file    conventions       YYYYMMDDHHMISS     
_FillValue                    "�   PLATFORM_NUMBER                	long_name         Float unique identifier    conventions       WMO float identifier : A9IIIII     
_FillValue                    "�   PTT                	long_name         .Transmission identifier (ARGOS, ORBCOMM, etc.)     
_FillValue                    "�   TRANS_SYSTEM               	long_name         "The telecommunications system used     
_FillValue                    #�   TRANS_SYSTEM_ID                	long_name         6The program identifier used by the transmission system     
_FillValue                     #�   TRANS_FREQUENCY                	long_name         ,The frequency of transmission from the float   units         hertz      
_FillValue                    #�   TRANS_REPETITION             	long_name         2The repetition rate of transmission from the float     units         second     
_FillValue        G�O�        #�   POSITIONING_SYSTEM                 	long_name         Positioning system     
_FillValue                    #�   CLOCK_DRIFT              	long_name         $The rate of drift of the float clock   units         decisecond/day     
_FillValue        G�O�        #�   PLATFORM_MODEL                 	long_name         Model of the float     
_FillValue                    #�   PLATFORM_MAKER                 	long_name         The name of the manufacturer   
_FillValue                    #�   INST_REFERENCE                 	long_name         Instrument type    conventions       Brand, type, serial number     
_FillValue                  @  $�   WMO_INST_TYPE                  	long_name         Coded instrument type      conventions       Argo reference table 8     
_FillValue                    %<   	DIRECTION                	long_name         Direction of the profiles      conventions       ;A: ascending profiles, B: descending and ascending profiles    
_FillValue                    %@   PROJECT_NAME               	long_name         .The program under which the float was deployed     
_FillValue                  @  %D   DATA_CENTRE                	long_name         3Data centre in charge of float real-time processing    conventions       Argo reference table 4     
_FillValue                    %�   PI_NAME                comment       "Name of the principal investigator     
_FillValue                  @  %�   ANOMALY                	long_name         :Describe any anomalies or problems the float may have had.     
_FillValue                    %�   LAUNCH_DATE                 	long_name         Date (UTC) of the deployment   conventions       YYYYMMDDHHMISS     
_FillValue                    &�   LAUNCH_LATITUDE              	long_name         #Latitude of the float when deployed    units         degrees_north      
_FillValue        @�i�       	valid_min         �V�        	valid_max         @V�             &�   LAUNCH_LONGITUDE             	long_name         $Longitude of the float when deployed   units         degrees_east   
_FillValue        @�i�       	valid_min         �f�        	valid_max         @f�             &�   	LAUNCH_QC                	long_name         )Quality on launch date, time and location      conventions       Argo reference table 2     
_FillValue                    &�   
START_DATE                  	long_name         -Date (UTC) of the first descent of the float.      conventions       YYYYMMDDHHMISS     
_FillValue                    &�   START_DATE_QC                	long_name         Quality on start date      conventions       Argo reference table 2     
_FillValue                    &�   DEPLOY_PLATFORM                	long_name         %Identifier of the deployment platform      
_FillValue                     '    DEPLOY_MISSION                 	long_name         2Identifier of the mission used to deploy the float     
_FillValue                     '    DEPLOY_AVAILABLE_PROFILE_ID                	long_name         7Identifier of stations used to verify the first profile    
_FillValue                    '@   END_MISSION_DATE                	long_name         -Date (UTC) of the end of mission of the float      conventions       YYYYMMDDHHMISS     
_FillValue                    (@   END_MISSION_STATUS               	long_name         )Status of the end of mission of the float      conventions       ,T:No more transmission received, R:Retrieved   
_FillValue                    (P   SENSOR        	            	long_name         List of sensors on the float   conventions       Argo reference table 3     
_FillValue                     (T   SENSOR_MAKER      	            	long_name         The name of the manufacturer   
_FillValue                    (t   SENSOR_MODEL      	            	long_name         Type of sensor     
_FillValue                    *t   SENSOR_SERIAL_NO      	            	long_name         The serial number of the sensor    
_FillValue                     ,t   SENSOR_UNITS      	            	long_name         2The units of accuracy and resolution of the sensor     
_FillValue                     ,�   SENSOR_ACCURACY       	         	long_name         The accuracy of the sensor     
_FillValue        G�O�        ,�   SENSOR_RESOLUTION         	         	long_name         The resolution of the sensor   
_FillValue        G�O�        ,�   	PARAMETER         	            	long_name         /List of parameters with calibration information    conventions       Argo reference table 3     
_FillValue                     ,�   PREDEPLOYMENT_CALIB_EQUATION      	            	long_name         'Calibration equation for this parameter    
_FillValue                    ,�   PREDEPLOYMENT_CALIB_COEFFICIENT       	            	long_name         *Calibration coefficients for this equation     
_FillValue                    .�   PREDEPLOYMENT_CALIB_COMMENT       	            	long_name         .Comment applying to this parameter calibration     
_FillValue                    0�   REPETITION_RATE                	long_name         &The number of times this cycle repeats     units         number     
_FillValue         ��        2�   
CYCLE_TIME                 	long_name         @The total time of a cycle : descent + parking + ascent + surface   units         decimal hour   
_FillValue        G�O�        2�   PARKING_TIME               	long_name         &The time spent at the parking pressure     units         decimal hour   
_FillValue        G�O�        2�   DESCENDING_PROFILING_TIME                  	long_name         .The time spent sampling the descending profile     units         decimal hour   
_FillValue        G�O�        2�   ASCENDING_PROFILING_TIME               	long_name         -The time spent sampling the ascending profile      units         decimal hour   
_FillValue        G�O�        2�   SURFACE_TIME               	long_name         The time spent at the surface.     units         decimal hour   
_FillValue        G�O�        2�   PARKING_PRESSURE               	long_name         !The pressure of subsurface drifts      units         decibar    
_FillValue        G�O�        2�   DEEPEST_PRESSURE               	long_name         5The deepest pressure sampled in the ascending profile      units         decibar    
_FillValue        G�O�        3    DEEPEST_PRESSURE_DESCENDING                	long_name         6The deepest pressure sampled in the descending profile     units         decibar    
_FillValue        G�O�        3Argo meta-data  2.2 1.2 20120521144513  20120521144513  13857   09335                                                                                                                                                                                                                                                           ARGOS           01670                                           B� 4ARGOS       PALACE_n/a      Webb                                                                                                                                                                                                                                                            PALACE_n/a_28                                                   845 A   ACCE (Atlantic Circulation and Climate Experiment)              AO  Bob Molinari                                                    n/a                                                                                                                                                                                                                                                             19970719145500  ?~���   �.+    1   19970719103000  1   R/V Seward Johnson              97-03                           CTD 108                                                                                                                                                                                                                                                                             TEMP            PRES            n/a                                                                                                                                                                                                                                                             Micron                                                                                                                                                                                                                                                          55032                                                                                                                                                                                                                                                           MP40-C-2000-G                                                                                                                                                                                                                                                   54              4969            degree_Celsius  decibar         G�O�G�O�G�O�G�O�TEMP            PRES            R=R1+R2*(CNTS/1000)+R3*(CNTS/1000)**2+R4*(CNTS/1000)**3 ; [1/(T1+T2*ln(R)+T3*ln(R)**3)]-273.15                                                                                                                                                                  PSLOPE*CNTS+POFF                                                                                                                                                                                                                                                r1=-0.622685; r2=11.4653; r3=0.0915065; r4=-0.0049331; ; t1=9.244042e-4;t2=2.226682e-4;t3=1.225617e-7;                                                                                                                                                          pslope=7.382186; poff=-74.9937;                                                                                                                                                                                                                                 temperature calibration date            n/a                                                                                                                                                                                                                     pressure calibration date               n/a                                                                                                                                                                                                                        C�  G�O�G�O�G�O�G�O�Dz  Dz  G�O�                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        