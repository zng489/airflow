# Airflow

- Unfortunately, AirFlow just works in linux system, then to work with it you have to do some procedure before. Let's get started!!.

https://www.youtube.com/watch?v=CLUhL6-RpqU BigDatapedia ML & DS

https://www.youtube.com/watch?v=M521KLHGaZc datastacktv

https://www.youtube.com/watch?v=i25ttd32-eo&t=769s


- Tutorial step by step:
  - Windows PowerShell > dism.exe /omline /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
  - Restart
  - Local Disck (C:) > Users > ZNG-Lenovo 
    - Creating AirFlow Folder
  - Ubuntu WSL
  - sudo apt update && sudo apt upgrade
  - sudo nano /etc/wsl/conf
  - GNU nano 4.8: 
    - [automount] 
    - root = / 
    - options = "metadata"
    - CTRL + S
    - CTRL + X
  - Acessing the folders
    - cd .. > ls > cd .. > ls > folder c   
  - cd home/<name>/, in this case cd home/ZNG-Lenovo/
  - python3 --version
  - sudo apt update
  - sudo apt install python3-pip
  - pip3 --version
  - pip3 install apache-airflow[gcp,statsd,sentry]==1.10.10
  - pip or pip3 install cryptography==2.9.2
  - pip3 install pyspark==2.4.5
  - export AIRFLOW_HOME=/c/Users/ZNG-Lenovo/airflow
  - nano ~/.bashrc
  - export AIRFLOW_HOME=/c/Users/ZNG-Lenovo/airflow
  - airflow version
  - echo $AIRFLOW_HOME
  - airflow initdb
  - airflow webserver
  
  
  
  



