# Minibase Relational DB → Vector Database with LSH Support  

<img width="560" height="334" alt="vector" src="https://github.com/user-attachments/assets/5726c23e-f35c-4a1e-9a84-92074b21a61f" />
 

## 📌 Overview  
This project extends the **MiniBase Relational Database System** into a **Vector-Aware Database** that supports **range queries** and **approximate nearest-neighbor (ANN) searches** over **100-dimensional vectors**.  

By integrating **Locality-Sensitive Hashing (LSH)** and a newly implemented **LSH-Forest index**, we enable **efficient similarity searches** that scale to high-dimensional data.  
In Phase 3, we further extend MiniBase with an **Index Nested Loop Join (INLJ) operator** and a **command-line interface** to make the system interactive and capable of managing complete vector-based datasets.  

---

## 🚀 Key Features  

### Phase 2  
- Support for **100D vector attribute type** (`Vector100Dtype`).  
- Implementation of **Euclidean distance-based comparisons** in queries.  
- **LSH-Forest Index (LSHFIndex)** for similarity search.  
- **Batch Insertion** of high-dimensional data.  
- Support for **Range Search** and **Nearest Neighbor (NN) Search**.  
- Performance analysis of LSH parameters (**h = #hashes per layer**, **L = #layers**).  

### Phase 3  
- New **Index Nested Loop Join (INLJ)** operator supporting joins over vector and scalar attributes.  
- **Command-Line Interface** with commands for:  
  - `open database` / `close database`  
  - `batchcreate` (create tables from data files)  
  - `createindex` (B-Tree or LSH)  
  - `batchinsert` / `batchdelete` (bulk updates)  
  - `query` (sort, filter, range, NN, distance joins)  
- Full support for **distance joins**, **top-k retrieval**, and **vector similarity queries**.  
- Extended **performance evaluation** (page reads/writes, recall, precision).  

---

## ⚙️ System Requirements  
- **OS:** Linux / macOS / WSL (recommended)  
- **Java:** JDK 8 or later  
- **Memory:** ≥4GB RAM (8GB recommended)  
- **Disk Space:** ≥2GB free space  
- **Dependencies:** Compile all MiniBase packages before running  

```bash
javac -cp . chainexception/*.java lshfindex/*.java global/*.java heap/*.java \
  diskmgr/*.java index/*.java btree/*.java bufmgr/*.java iterator/*.java catalog/*.java
