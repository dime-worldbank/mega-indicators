# Databricks notebook source
import os
import sys
current_dir = os.getcwd()
sys.path.append(os.path.join(current_dir, "utils"))

# COMMAND ----------

from utils import helper as hp
hp.please()
