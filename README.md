# BridgeScope: A Universal Toolkit for Bridging Large Language Models and Databases

This is the open-source code implementation for the paper "BridgeScope: A Universal Toolkit for Bridging Large Language Models and Databases". 

BridgeScope is a universal toolkit that bridges LLMs and databases towards greater usability, efficiency, and security guarantees. It aims to help general-purpose LLM agents handling data-related tasks more effectively and efficiently. See details in [BridgeScope server](servers/bridgescope/readme.md). This repo provides an implementation of BridgeScope for PostgreSQL based on the Model Context Protocol (MCP), and all necessary data and codes to reproduce the experimental results in the paper.

## 📋 Table of Contents

- [Overview of BridgeScope](#-overview-of-bridgescope)
- [Project Structure](#-project-structure)
- [Requirements](#-requirements)
- [Environment Setup](#-environment-setup)
- [Run the Code](#-run-the-code)
- [Contact](#-contact)
- [Acknowledgements](#-acknowledgements)

## 🎯 Overview of BridgeScope

As large language models (LLMs) demonstrate increasingly powerful reasoning and orchestration capabilities, LLM-based agents are rapidly proliferating for complex data-related tasks. Despite this progress, the current design of how LLMs interact with databases exhibits critical limitations in usability, security, privilege management, and data transmission efficiency. 
BridgeScope incorporates three core innovations to address these challenges:


#### **1. Fine-Grained Tooling.**
Modularizes SQL operations into fine-grained tools for **context retrieval**, **CRUD execution**, and **ACID-compliant transaction management**, enabling more precise and LLM-friendly control over database functionalities.

#### **2. Multi-level Security Guarantees**

Customizes tool implementations based on both **database-side user privileges** and **user-side security policies**. Both prompts the LLM planning within authorized and secure operational boundaries and employs rule-based verifications to strictly block unauthorized access and risky operations. 

#### **3. Direct Data Routing**

Introduces proxy-based mechanisms that enables seamless inter-tool data transmission, eliminating LLM transmission bottlenecks. See [MCP-Proxy-Execute](servers/ProxyServer/README.md) for details.

Evaluations on two novel benchmarks demonstrate that BridgeScope enables LLM agents to operate databases more effectively, reduces token usage by up to 80% through improved security awareness, and uniquely supports data-intensive workflows beyond existing toolkits.

## 📁 Project Structure

```
bridgescope/
├── scripts/                 # Database scripts
│   ├── backup_postgres.sh   # Database backup script
│   └── restore_postgres.sh  # Database restore script
├── test/                    # Test code
│   ├── evaluate_privilege.py    # Privilege evaluation
│   ├── evaluate_proxy.py        # Proxy evaluation
│   └── ...                     # Others
├── benchmark/              # Benchmark data
│   ├── nl2trans_sql/       # NLP2SQL test data
│   └── proxy/             # Proxy test data
├── servers/               # Server implementations
└── ...                    # Others
```

## 🔧 Requirements

- Python 3.10
- PostgreSQL 13.12
- OpenAI API Key or Claude API Key
- Recommended: use conda or venv to create a virtual environment

## 📦 Environment Setup

### 1. Clone the Repository
```bash
git clone git@github.com:duoyw/bridgescope.git
cd bridgescope
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure PostgreSQL Database

#### 3.1 Ensure PostgreSQL is installed and running
```bash
# Check if PostgreSQL is installed
which psql
```

#### 3.2 Ensure the existence of the postgres user and database

#### 3.3 Download Data Files
```bash
# Create data directory
mkdir -p /path/to/your/backup_data

# Download database data files
wget -O /path/to/your/backup_data/backup_db.zip "https://github.com/duoyw/bridgescope/releases/download/db/backup_db.zip"

# Extract data files
cd /path/to/your/backup_data
unzip backup_db.zip
```

#### 3.4 Configure Restore Script Path
Edit the `scripts/restore_postgres.sh` file and modify the `BACKUP_DIR` variable:
```bash
# Open the script file
vim scripts/restore_postgres.sh

# Find and modify this line (around line 15) to your actual data file path
BACKUP_DIR="/path/to/your/backup_data/backup_20250801_165108"
PGPASSWORD="your_postgres_password"
PGUSER="postgres"
PGHOST="localhost"
PGPORT="5432"
```

#### 3.5 Run the Database Restore Script
```bash
# Grant execute permission to the script
chmod +x scripts/restore_postgres.sh

# Run the restore script
./scripts/restore_postgres.sh
```

### 4. Configure API Keys

#### 4.1 Edit Model Configuration File
Edit the `test/model_config.json` file to set your API keys:

```bash
# Open the config file
vim test/model_config.json
```

Modify the API key configuration in the file:
```json
[
  {
    "config_name": "gpt-4o",
    "model_type": "openai_chat",
    "model_name": "chatgpt-4o-latest",
    "api_key": "your-openai-api-key-here",  // Replace with your OpenAI API key
    "client_args": {
      "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1"
    }
  },
  {
    "config_name": "claude-4",
    "model_type": "openai_chat",
    "model_name": "claude-sonnet-4-20250514",
    "api_key": "your-claude-api-key-here",  // Replace with your Claude API key
    "client_args": {
      "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1"
    }
  }
]
```

#### 4.2 Edit MCP Server Configuration
Edit the `test/mcp.json` file and update all Python paths to the actual paths on your system:

```bash
# Open the MCP config file
vim test/mcp.json
```

Then replace all Python paths in the `command` fields with your actual paths.


## 🚀 Run the Code

### 1. Privilege Evaluation Test

Test the effect of access control for different user types and operation types:

```bash
cd test
# Test administrator user
python evaluate_privilege.py --user administrator --llm gpt-4o --algo bridgescope --op select --n_samples 5

# Test normal user
python evaluate_privilege.py --user normal --llm claude-4 --algo pg-mcp --op insert --n_samples 10

# Test irrelevant user
python evaluate_privilege.py --user irrelevant --llm gpt-4o --algo bridgescope --op update --n_samples 5
```

Parameter description:
- `--user`: User type (administrator/normal/irrelevant)
- `--llm`: Language model to use (gpt-4o/claude-4)
- `--algo`: Algorithm type (bridgescope/pg-mcp)
- `--op`: Operation type (select/insert/delete/update)
- `--n_samples`: Number of test samples

### 2. Proxy Mechanism Evaluation

Test the effectiveness of the proxy mechanism:

```bash
cd test

# Test BridgeScope proxy
python evaluate_proxy.py --llm gpt-4o --algo bridgescope --n_samples 5

# Test PG-MCP-S proxy
python evaluate_proxy.py --llm claude-4 --algo pg-mcp-s --n_samples 5
```

## 📞 Contact

If you have any questions or suggestions, please contact us via:

- Submit an Issue
- Email the project maintainer

## 🙏 Acknowledgements

Thanks to all researchers and developers who contributed to this project.

---

**Note**: Please make sure the database connection and API keys are properly configured before use. It is recommended to verify in a test environment first.
