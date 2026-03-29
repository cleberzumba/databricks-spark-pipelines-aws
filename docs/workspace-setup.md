# Databricks Workspace Setup (AWS)

## Overview

This document describes the process of creating a Databricks workspace on AWS using the Account Console.

---

## Prerequisites

- AWS Account
- Access to Databricks Account Console
- Permissions to create IAM roles and S3 resources

---

## Steps to Create the Workspace

### 1. Access Databricks Account Console

- Log in to Databricks Account Console
- Navigate to **Workspaces**

---

### 2. Create New Workspace

- Click **Create Workspace**
- Fill in:

| Field           | Value              |
|-----------------|-------------------|
| Workspace Name  | ws-spark          |
| Region          | us-east-1         |

---

### 3. Configure Cloud Resources

Instead of manual configuration, the workspace was created using automatic resource provisioning.

Databricks automatically created:

- S3 bucket for storage
- IAM roles for compute and storage
- Access policies

---

### 4. Review AWS Resources

During creation, Databricks provisions:

- Storage Role
- Compute Role
- S3 Bucket
- Access Policies

---

### 5. Create Workspace

- Click **Initiate workspace creation**
- Wait for deployment (~5–10 minutes)

---

## Result

The workspace is successfully created with:

- Managed VPC
- Configured IAM roles
- Integrated S3 storage

---

## Notes

- Manual IAM configuration was avoided to reduce errors
- Databricks-managed setup ensures correct permissions

---

## Next Step

- Create and configure cluster
- Start data processing with PySpark
