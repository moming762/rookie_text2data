## rookie_text2data

**Author:** jaguarliuu
**Version:** 0.2.1
**Type:** tool

### Description
A tool that converts natural language into secure and optimized SQL queries, supporting both MySQL and PostgreSQL databases.


### 声明
We are truly grateful for the overwhelming interest in this experimental project. Your feedback is invaluable for improving this plugin. Join our WeChat group for discussions and collaboration opportunities!

![WeChat](./_assets/1.png)

### ✨ Core Features

#### ​Multi-Database Support
- Native support for MySQL and PostgreSQL syntax differences
- Automatic SQL syntax adaptation based on database type (e.g., LIMIT vs FETCH FIRST)
#### ​Security Mechanisms
- Mandatory result set limits (default LIMIT 100)
- DML operation prohibition (SELECT statements only)
- Field whitelist validation (based on database metadata)
- Least privilege principle for query execution

### Supported Databases
MySQL
PostgreSQL

### Supported LLMs
Compatible with ​all non-deep-thinking models
- ChatGLM-6B
- DeepSeek V3
- Qwen-max
...

### Quick Start
#### SQL Generation Component
1. Import the rookie_text2data plugin
2. Configure basic parameters:

| Parameter      | Type           | Required | Description                                       | Multilingual Support     |
|----------------|----------------|----------|---------------------------------------------------|--------------------------|
| db_type        | select         | Yes      | Database type (MySQL/PostgreSQL)                  | CN/EN/PT                |
| host           | string         | Yes      | Database host/IP address                          | CN/EN/PT                |
| port           | number         | Yes      | Database port (1-65535)                           | CN/EN/PT                |
| db_name        | string         | Yes      | Target database name                              | CN/EN/PT                |
| table_name     | string         | No       | Comma-separated table names (empty for all tables)| CN (format hints)       |
| username       | string         | Yes      | Database username                                 | CN/EN/PT                |
| password       | secret-input   | Yes      | Database password                                 | CN/EN/PT                |
| model          | model-selector | Yes      | LLM model configuration                           | CN/EN/PT                |
| query          | string         | Yes      | Natural language query statement                  | CN/EN/PT                |

3. Select Model，We recommend using the Qwen-max model. Other models can be tested but deep-thinking models are unsupported.
4. Generate SQL queries using natural language

#### SQL Execution Component
1. Import the rookie_execute_sql plugin
2. Configure basic parameters:

| Parameter     | Type     | Required | Description                              | Multilingual Support     |
|---------------|----------|----------|------------------------------------------|--------------------------|
| db_type       | select   | Yes      | Database type (MySQL/PostgreSQL)         | CN/EN/PT                |
| host          | string   | Yes      | Database host/IP address                 | CN/EN/PT                |
| port          | number   | Yes      | Database port (1-65535)                  | CN/EN/PT                |
| db_name       | string   | Yes      | Target database name                     | CN/EN/PT                |
| sql           | string   | Yes      | SQL query to execute                     | CN/EN/PT                |

3. Click "Execute" to run the SQL statement