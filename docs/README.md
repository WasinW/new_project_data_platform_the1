# GCP Data Platform Documentation

## 📚 Overview
This documentation covers a **hybrid batch/realtime data platform** migrating from AWS S3 to GCP, featuring **shared infrastructure** design with domain-specific data isolation.

## 🏗️ Architecture Highlights
- **4 Pipeline Types**: Initiate (migration), Realtime (streaming), Batch (hourly), Reconciliation (validation)
- **Shared Infrastructure**: Common datasets, buckets, topics across all domains
- **Domain Isolation**: Data separated by table prefixes and subfolders
- **Orchestration**: Cloud Composer (Airflow) with Dataflow processing

---

## 📁 Documentation Structure

```
docs/
├── README.md                          # This overview
├── Guildline.md                       # Complete implementation guide
├── configuration/
│   ├── parameters.conf                # Shared infrastructure parameters
│   └── README.md                      # Configuration setup guide
├── architecture/
│   ├── pipeline-oop-design.md         # OOP design for all pipelines
│   └── pipeline-class-diagrams.md     # UML diagrams and sequences
└── deployment/
    └── infrastructure-prerequisites.md # Infrastructure setup requirements
```

---

## 🚀 Quick Start

### **1. Infrastructure Setup**
```bash
# Configure shared infrastructure parameters
cp docs/configuration/parameters.conf.example docs/configuration/parameters.conf
# Edit with your project values

# Deploy shared infrastructure
terraform init && terraform plan && terraform apply
```

### **2. Domain Configuration**
```bash
# Add your domain to supported_domains in terraform/variables.tf
# Deploy domain-specific resources (table schemas, DAGs)
```

### **3. Pipeline Deployment**
```bash
./scripts/setup.sh  # Deploys all 4 pipeline types
```

---

## 📋 Shared Infrastructure Design

### **Shared Resources** (No Domain Prefix)
- **Datasets**: `raw_data`, `staging_data`, `monitoring_data`
- **Buckets**: `{project-id}-dataflow-temp`, `{project-id}-gcs-staging`
- **Topics**: `data-events-create`, `data-events-update`
- **Composer**: `composer-{environment}`

### **Domain-Specific Elements**
- **Table Names**: `{domain}_batch_input`, `{domain}_realtime_events`
- **DAGs**: `{pipeline_type}_{domain}_pipeline`
- **Subfolders**: `/{domain}/` within shared buckets
- **Monitoring**: `{domain}_processing_errors`, `{domain}_validation_results`

---

## 📖 Key Documents

| Document | Purpose | Audience |
|----------|---------|----------|
| **[Guildline.md](Guildline.md)** | Complete implementation guide | All team members |
| **[Configuration Guide](configuration/README.md)** | Parameter setup and management | DevOps, Engineers |
| **[OOP Design](architecture/pipeline-oop-design.md)** | Pipeline architecture details | Developers |
| **[Class Diagrams](architecture/pipeline-class-diagrams.md)** | UML and sequence diagrams | Architects |

---

## 🎯 Pipeline Types

### 1. **Initiate Pipeline** (`initiate_pipeline.py`)
- **Purpose**: One-time S3→BigQuery migration
- **Technology**: Pure Airflow (no Dataflow)
- **Scope**: Historical data migration

### 2. **Realtime Pipeline** (`realtime_trigger.py`)
- **Purpose**: Continuous Pub/Sub processing
- **Technology**: Airflow + Dataflow streaming
- **Scope**: Live event processing

### 3. **Batch Pipeline** (`batch_pipeline.py`)
- **Purpose**: Hourly batch processing
- **Technology**: Airflow + Dataflow batch
- **Scope**: Transitional processing

### 4. **Reconciliation Pipeline** (`reconciliation_pipeline.py`)
- **Purpose**: Daily validation against S3
- **Technology**: Airflow + Dataflow
- **Scope**: Data quality assurance

---

## 🔧 Configuration Management

**YAML-first approach** with environment variable substitution:
- Main config: `config/pipeline_config.yaml`
- Airflow variables: `airflow/config/airflow_variables.json`
- Terraform: `terraform/variables.tf`
- Parameter templates: `docs/configuration/parameters.conf`

**Critical Pattern**: `distribution_mapping` controls data flow from single records to multiple BigQuery tables.

---

## 🚦 Getting Started Checklist

- [ ] Review [Guildline.md](Guildline.md) for complete overview
- [ ] Configure parameters in `docs/configuration/parameters.conf`
- [ ] Deploy shared infrastructure with Terraform
- [ ] Add your domain to `supported_domains`
- [ ] Deploy pipeline DAGs to Composer
- [ ] Validate with sample data processing
- [ ] Monitor through BigQuery audit tables

---

For detailed implementation guidance, see **[Guildline.md](Guildline.md)**.
1. **[Monitoring Guide](operations/monitoring-guide.md)** - Set up monitoring and alerting
2. **[Maintenance Procedures](operations/maintenance-procedures.md)** - Regular operational tasks
3. **[Disaster Recovery](operations/disaster-recovery.md)** - DR procedures and runbooks

---

## 📖 Document Descriptions

### 🚀 **1. Vibe Coding Solution** 
**File**: `vibe-coding-solution.md`  
**Purpose**: Comprehensive guide covering all aspects of modern data platform development
- Architecture design patterns
- Code organization & structure
- Pipeline development best practices
- Error handling & monitoring
- Testing strategies
- Deployment & CI/CD
- Performance optimization
- Security best practices

### 🏗️ **2. Infrastructure Prerequisites**
**File**: `deployment/infrastructure-prerequisites.md`  
**Purpose**: Complete list of required GCP services and infrastructure components
- Google Cloud project setup
- IAM & service accounts configuration
- BigQuery datasets and tables
- Pub/Sub topics and subscriptions
- Cloud Storage buckets
- Secret Manager setup
- Dataplex configuration
- Monitoring & alerting resources
- Networking & security settings

### ⚙️ **3. Parameters Configuration**
**File**: `configuration/parameters.conf`  
**Purpose**: Single source of truth for all configurable parameters
- Project and regional settings
- Service account configurations
- Resource names and identifiers
- Performance and scaling parameters
- Security and networking settings
- Feature flags and toggles
- Environment-specific overrides

---

## 🔧 Configuration Process

### Step 1: Set Up Infrastructure
```bash
# Review infrastructure requirements
cat docs/deployment/infrastructure-prerequisites.md

# Set up GCP project and enable APIs
gcloud projects create ${PROJECT_ID}
gcloud services enable bigquery.googleapis.com dataflow.googleapis.com # ... etc
```

### Step 2: Configure Parameters
```bash
# Copy and edit parameters file
cp docs/configuration/parameters.conf docs/configuration/parameters.local.conf
# Edit parameters.local.conf with your actual values

# Run parameter replacement
bash scripts/replace_parameters.sh
```
---

## 🔧 Quick Deployment Guide

### Step 1: Configure Shared Infrastructure
```bash
# Set up shared infrastructure parameters
cp docs/configuration/parameters.conf.example docs/configuration/parameters.conf
# Edit with your project-specific values

# Key shared infrastructure parameters:
PROJECT_ID="your-gcp-project-id"
REGION="asia-southeast1"
ENVIRONMENT="production"

# Shared datasets (no domain prefix)
BIGQUERY_DATASET_RAW="raw_data"
BIGQUERY_DATASET_STAGING="staging_data"
BIGQUERY_DATASET_MONITORING="monitoring_data"
```

### Step 2: Deploy Shared Infrastructure
```bash
# Initialize and deploy Terraform
cd terraform
terraform init
terraform plan
terraform apply

# This creates:
# - Shared datasets across all domains
# - Shared storage buckets
# - Shared Pub/Sub topics
# - Single Composer environment
# - Shared Dataplex lake and zones
```

### Step 3: Configure Domain
```bash
# Add your domain to supported_domains in terraform/variables.tf
# Deploy domain-specific resources (table schemas, subscriptions)
terraform apply
```

### Step 4: Deploy Pipelines
```bash
# Deploy all 4 pipeline types for your domain
./scripts/setup.sh

# This deploys:
# - initiate_{domain}_pipeline
# - realtime_{domain}_pipeline  
# - batch_{domain}_pipeline
# - reconciliation_{domain}_pipeline
```

---

## � Shared Infrastructure Benefits

### **Cost Optimization**
- **85% cost reduction** through shared resources
- Single Composer environment serves all domains
- Shared storage buckets with domain subfolders
- Consolidated monitoring and alerting

### **Management Simplification**
- **Single point of control** for infrastructure
- Domain-agnostic resource management
- Centralized security and access control
- Unified monitoring dashboards

### **Scalability Enhancement**
- **Add new domains** without infrastructure changes
- Automatic resource sharing optimization
- Horizontal scaling across domains
- Future-proof architecture design

---

For detailed implementation guidance, see **[Guildline.md](Guildline.md)**.

### **Infrastructure Setup Scripts**
- `scripts/setup.sh` - Complete infrastructure setup
- `scripts/setup_secrets.sh` - Secret Manager configuration
- `scripts/lean_cleanup.sh` - Project cleanup and validation

### **Terraform Modules**
- Modular infrastructure definitions
- Environment-specific variable files
- State management configuration

---

## 📋 Usage Checklist

### Pre-Deployment Checklist
- [ ] GCP project created with billing enabled
- [ ] All required APIs enabled
- [ ] Parameters configured in `parameters.conf`
- [ ] Infrastructure prerequisites reviewed
- [ ] Service account permissions verified

### Deployment Checklist
- [ ] Parameters replaced using replacement script
- [ ] Terraform state backend configured
- [ ] Infrastructure deployed successfully
- [ ] Pipelines deployed and tested
- [ ] Monitoring and alerting configured

### Post-Deployment Checklist
- [ ] End-to-end pipeline testing completed
- [ ] Monitoring dashboards accessible
- [ ] Alert notifications working
- [ ] Documentation updated with actual values
- [ ] Team training completed

---

## 🆘 Support and Troubleshooting

### Common Issues
1. **Parameter replacement failures** - Check `parameters.conf` syntax
2. **Permission errors** - Verify service account roles
3. **Resource conflicts** - Check for existing resources with same names
4. **Network connectivity** - Verify VPC and firewall configurations

### Getting Help
- Check `docs/deployment/troubleshooting.md` for common solutions
- Review logs in Cloud Console
- Validate configurations using provided scripts
- Consult the architecture documentation for design clarifications

---

## 🔄 Maintenance and Updates

### Regular Maintenance
- Review and update parameters as needed
- Monitor resource usage and costs
- Update documentation with operational changes
- Refresh credentials and secrets

### Version Updates
- Update parameter file with new features
- Test changes in development environment
- Update documentation and runbooks
- Deploy updates using CI/CD pipeline

---

## 📞 Contact and Support

For questions, issues, or contributions:
- **Team**: Data Platform Engineering
- **Email**: data-platform@${DOMAIN}
- **Documentation**: This repository
- **Runbooks**: `docs/operations/`

---

*Last Updated: $(date)*  
*Documentation Version: 1.0.0*
