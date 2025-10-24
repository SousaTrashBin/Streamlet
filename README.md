# StreamLet Project 

## Group Number 3

| Name           | Number |
|----------------|--------|
| Diogo Sousa    | 59792  |
| Denis Bahnari  | 59878  |
| Bruno Faustino | 59784  |


---

## Project Description

The project was developed in **Java**, more specifically in **version 25**. 
The entire code structure is in the folder **`StreamLet/`**, which corresponds to the project **root**.

---

## Compilation Instructions


Inside the `StreamLet/` folder, there is a **Makefile** that automates the build process. 
To compile the project, just run on the terminal:

```bash
make
```

---

## Running the System
Inside the StreamLet/out/ folder, there is the main Streamlet class, which can be started with the following command:
```bash
java -cp out Streamlet <id>
```
where **id** represents the node identifier. 
The project currently supports exactly 5 nodes. 
In order for the system to work properly, it is necessary to start five different instances, each with a distinct ID (for example, from 0 to 4):

```bash
java -cp out Streamlet 0
java -cp out Streamlet 1
java -cp out Streamlet 2
java -cp out Streamlet 3
java -cp out Streamlet 4
```

Another easy way to start the system is by running the provided shell script(Linux Ubuntu 24):
```bash
./run_all.sh
```
This script automatically launches all five nodes (IDs 0–4) in separate processes,
allowing the project to start and operate correctly without manual intervention.

If permission is denied, execute:
```bash
chmod +x run_all.sh
```

Currently, the project support transactions by simulations of the corresponding node leader, or by a client process.
Transaction generation mode can be changed in the **config.txt** file in **transactionsMode** section.

To execute the client process run:
```bash
java -cp out StreamletClient
```
