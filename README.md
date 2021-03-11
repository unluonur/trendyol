# Application
## Build the application
```
sbt clean assembly
```

## We need to make the output directory writeable
```
chmod -R 777 output
```

## Run
```
sudo docker-compose up
```

# System Design
![alt text](data-processing.jpg)
