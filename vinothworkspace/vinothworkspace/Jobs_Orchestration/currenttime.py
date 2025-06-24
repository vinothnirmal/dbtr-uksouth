# Databricks notebook source
time_obj = dbutils.jobs.taskValues.get(taskKey = '01_current_datetime',key= 'input_time')
print(time_obj)
