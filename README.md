
# R Library

The R library folder **//fileserver1/ProductManagement/Manos/Softone_data/Soft1R/**  contains all necessary folders and files to compile the 
library. The important Folders and files are the following:  

* **//fileserver1/ProductManagement/Manos/Softone_data/Soft1R/R/** This folder contains all functions of the library. Each function 
contained in a different R script file. Every file is fully documented.  
* **//fileserver1/ProductManagement/Manos/Softone_data/Soft1R/DESCRIPTION** This file contains basic information of the library 
e.g. creator, version and lists all R libraries used in all functions  
* **//fileserver1/ProductManagement/Manos/Softone_data/Soft1R/tests/** This folder contains testing files, run while the compilation
of the library  
* **//fileserver1/ProductManagement/Manos/Softone_data/Soft1R/data/** This folder contains datasets used during the testing phase.  
* **./Soft1R.Rcheck/** This folder is created automatically during the library 
compilation.  


## Soft1 BI Analytics service Files

The folder path is **//fileserver1/ProductManagement/Manos/Softone_data/All_analytics**. It contains various scripts & files used
for the Soft1 BI predictive analytics service.  

* **//fileserver1/ProductManagement/Manos/Softone_data/All_analytics/All_analytics.R** This is the R script that runs in the cloud
analytics server every 1 minute(CRON scheduler), checks for new installations needed to run analytics
and act appropriately.  
* **//fileserver1/ProductManagement/Manos/Softone_data/All_analytics/Analytics_Service_email.R** This is the R script that runs in 
the cloud analytics server every morning at 6:00 UTC, renders the **Analytics_Service.Rmd** 
file and sends the html report to several recipients.  
* **//fileserver1/ProductManagement/Manos/Softone_data/All_analytics/Analytics_Service.Rmd** This is an R markdown file that creates
a report of the last 24 hours activity of the server.  
* **//fileserver1/ProductManagement/Manos/Softone_data/All_analytics/sendgrid_creds** This is a file containg credentials information
used to sent emails through sengrid.  
* **//fileserver1/ProductManagement/Manos/Softone_data/All_analytics/Soft1R_0.1.0.tar.gz** This is the compiled Soft1R R library
(last version).


## Softone power-bi tools

The folder path **//fileserver1/ProductManagement/Manos/Softone_data/Power-bi/** contains 3 power bi tools.   

* **//fileserver1/ProductManagement/Manos/Softone_data/Power-bi/Leads Analysis.pbix** is a tool for presenting and measure performance
from leads & web trials from Fani. Created with A. Karantonis & K. Galitis   
* **//fileserver1/ProductManagement/Manos/Softone_data/Power-bi/Soft1 Log Stats.pbix** is a tool to analyze the logs from installations
in FANI, using data from XINSTLOGS. Created with G. Tsomokos  
* **//fileserver1/ProductManagement/Manos/Softone_data/Power-bi/Prosvasis.pbix** is a tool to measure performance of Prosvasis Go 
Installations. Created with G. Giorgoulakis, I. Kotsogianni & M. Kosmas


## Docker Soft1 BI Analytics service Files

The folder path **//fileserver1/ProductManagement/Manos/Softone_data/docker_analytics_service/** contains all necessary files to 
create a docker container to run the analytics service.   

* **//fileserver1/ProductManagement/Manos/Softone_data/docker_analytics_service/Docker_analytics.R ** This is the R script that
when the container starts, runs all analytics for the provided serial number.   
* **//fileserver1/ProductManagement/Manos/Softone_data/docker_analytics_service/Dockerfile ** This is the Dockerfile that creates the docker container.   
* **//fileserver1/ProductManagement/Manos/Softone_data/docker_analytics_service/sendgrid_creds** This is a file containg credentials information
used to sent emails through sengrid.  
* **//fileserver1/ProductManagement/Manos/Softone_data/docker_analytics_service/Soft1R_0.1.0.tar.gz** This is the compiled Soft1R R library
(last version).


## Azure VM analytics server details 

* Ubuntu Version 18.04  
* R Version 4.0.3  

## Useful Linux commands  

* **find . -type f -iname \*.txt -delete**  {Deletes all files with .txt extension in current folder}
* **find . -type f -iname \*.log -delete**  {Deletes all files with .log extension in current folder}   
* **R -e "install.packages('/home/analytics/docker/docker_all_analytics/Soft1R_0.1.0.tar.gz', repos = NULL, type = 'source')"** {Installs the Soft1R Library}   
* **R CMD BATCH --no-save --no-restore /home/analytics/docker/docker_all_analytics/All_analytics.R Logs/`date "+%Y_%m_%d_%H_%M_%S"`.log** {Runs the analytics jobs R script}   
* **R CMD BATCH --no-save --no-restore /home/analytics/docker/docker_all_analytics/Analytics_Service_email.R** {Run the daily server activity report}  






