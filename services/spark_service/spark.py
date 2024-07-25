#%%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession.builder.appName("My spark app").getOrCreate()

#%%
print(spark)
# %%
import pandas as pd
import numpy as np


data = {
    'id': range(1, 101),
    'name': ['Name' + str(i) for i in range(1, 101)],
    'age': np.random.randint(20, 60, size=100),
    'salary': np.random.uniform(30000, 100000, size=100),
    'date_of_birth': pd.date_range(start='1960-01-01', periods=100, freq='365D'),
    'department': np.random.choice(['Sales', 'Marketing', 'Finance', 'HR', 'IT'], size=100),
    'has_parking': np.random.choice([True, False], size=100),
    'latitude': np.random.uniform(-90, 90, size=100),
    'longitude': np.random.uniform(-180, 180, size=100),
    'rating': np.random.uniform(1, 5, size=100),
    'gender': np.random.choice(['M', 'F'], size=100),
    'married': np.random.choice([True, False], size=100),
    'join_date': pd.date_range(start='2010-01-01', periods=100, freq='365D'),
    'last_promotion_date': pd.date_range(start='2015-01-01', periods=100, freq='365D'),
    'email': ['name' + str(i) + '@example.com' for i in range(1, 101)],
    'phone_number': ['123-456-789' + str(i % 10) for i in range(1, 101)],
    'address': ['Address ' + str(i) for i in range(1, 101)],
    'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], size=100),
    'state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ'], size=100),
    'country': ['USA'] * 100,
    'zipcode': [str(10000 + i) for i in range(100)],
    'comments': ['Comment ' + str(i) for i in range(1, 101)],
    'performance_score': np.random.randint(60, 100, size=100),
    'bonus': np.random.uniform(1000, 10000, size=100),
    'manager_id': np.random.randint(100, 110, size=100),
    'contract_type': np.random.choice(['Full-Time', 'Part-Time', 'Contract'], size=100),
    'annual_leave_taken': np.random.randint(0, 30, size=100),
    'education_level': np.random.choice(['Bachelor', 'Master', 'PhD'], size=100),
    'job_title': np.random.choice(['Analyst', 'Manager', 'Executive', 'Specialist'], size=100),
    'office_location': np.random.choice(['Building A', 'Building B', 'Building C', 'Building D'], size=100),
    'project_count': np.random.randint(1, 10, size=100),
    'skills': [str(['Skill' + str(j) for j in range(1, 4)]) for i in range(100)],
    'certifications': [str(['Cert' + str(j) for j in range(1, 3)]) for i in range(100)],
    'work_hours': np.random.uniform(30, 50, size=100),
    'experience_years': np.random.randint(1, 20, size=100),
    'performance_review_date': pd.date_range(start='2022-01-01', periods=100, freq='365D'),
    'team_name': np.random.choice(['Team Alpha', 'Team Beta', 'Team Gamma', 'Team Delta'], size=100),
    'team_size': np.random.randint(5, 15, size=100),
    'is_remote': np.random.choice([True, False], size=100),
    'employee_status': np.random.choice(['Active', 'Inactive'], size=100)
}

df = pd.DataFrame(data)

#%%
df.head()

# %%
