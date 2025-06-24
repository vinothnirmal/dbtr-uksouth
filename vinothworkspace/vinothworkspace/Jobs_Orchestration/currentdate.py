# Databricks notebook source
date_obj = dbutils.jobs.taskValues.get(taskKey = '01_current_datetime',key= 'input_date')
print(date_obj)
