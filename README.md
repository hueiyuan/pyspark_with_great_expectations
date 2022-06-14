# pyspark_with_great_expectations
pyspark dataframe data quality with great_expectations framework

## Operation Steps
Step1. Install required libs
```
pip3 install -r requirements.txt
```

Step2. Execute setup.py install to install custom helpers. (> python3.8)
```
python3 setup.py install
```

Step3. Update related s3 path
Because this example emulate data which stored on AWS S3, need to update related bucket and prefix object in pyspark_main.py

Step4. Generate corresponding great_expectation suite json
```
python3 generate_expectation_suite.py --environment develop
```

Step5. Execute Spark main script
```
python3 pyspark_main.py --environment develop
```

## What is the next?
Becuase the repo just is a example, if you need to fork or refercence this module. Please refer to related document to modify.

## Reference
* [greate_expectations document](https://docs.greatexpectations.io/docs/)
