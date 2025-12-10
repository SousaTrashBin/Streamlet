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
In order for the system to work properly, it is necessary to start up to five different instances, each with a distinct ID (for example, from 0 to 4):

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

To execute the client process run:
```bash
java -cp out StreamletClient
```
## Features

All required features were implemented along with several additional enhancements:

1. **Persistence**: The blockchain maintains disk-based persistence for each node in the output folder using a dual-file
   approach. The primary file contains a snapshot of the in-memory data structures, updated every N epochs (currently
   10). A secondary log file records all operations between snapshots. This design ensures full crash recovery
   capability.

2. **Client Software**: A dedicated client application (StreamletClient class) enables external clients to submit
   transactions to any active server node. The node incorporates these transactions into blocks, replacing the default
   dummy transaction mechanism.

3. **Synchronization and Recovery**: A catch-up mechanism allows nodes to recover from crashes or network partitions.
   Nodes automatically detect gaps in their blockchain, identify missing epoch ranges, and request the corresponding
   blocks from peers using `JOIN` and `UPDATE` messages.

4. **Orphan Block Management**: The system includes robust handling of out-of-order block delivery. Blocks received
   before their parent blocks are buffered temporarily and automatically processed once the missing parent arrives,
   ensuring chain consistency despite network delays.

## Limitations
We found no limitations with our current implementation of this project.