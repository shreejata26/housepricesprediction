# -*- coding: utf-8 -*-
"""housing_price_prediction.ipynb

Automatically generated by Colaboratory.
"""

from __future__ import print_function
import pandas as pd
import numpy as np
import pickle
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt

import keras
from keras import metrics
from keras import regularizers
from keras.models import Sequential
from keras.layers import Dense, Dropout, Flatten, Activation
from keras.layers import Conv2D, MaxPooling2D
from keras.optimizers import Adam, RMSprop
from keras.callbacks import TensorBoard, EarlyStopping, ModelCheckpoint
from keras.utils import plot_model
from keras.models import load_model

# Reading the input file containing the features and target variable
# from google.colab import files
# uploaded = files.upload()
# import io
# data = pd.read_csv(io.StringIO(uploaded['data.csv'].decode('utf-8')))
data = pd.read_csv('Code_no_agg/data/data.csv')
data['REF_DATE_INT'] = data.REF_DATE.map(lambda x: int(x.replace('-', '')))

# Removing the input Unit and Scale related columns from the file

relevant_columns = np.array(data.columns.isin(['GEO', 'REF_DATE', 'DGUID'])) + np.array(data.dtypes != object)
input = data.loc[:, relevant_columns]
print(input.columns)

# DGUID of province we are interested in predicting the housing index for.

province_dguid = ['2016A000235', '2016A000259', '2016A000224', '2016A000248', '2016A000212', '2016A000247', '2016A000246']
# ON, BC, QB, AL, NS, SK, MT

# One-hot encode the province name

input1 = input[input['DGUID'].isin(province_dguid)]
province_data = pd.concat([input1, pd.get_dummies(input1.GEO, prefix='Province')], axis=1).sort_values(['REF_DATE_INT', 'GEO'])
print(province_data.columns)

# Storing list of columns to be used as features and target for our model into feature_columns and target_column.

feature_columns = province_data.columns[np.array(province_data.dtypes != object)][3:]
target_column = 'total_house_land'
print(feature_columns)

# Droping the data if target variable is null

def drop_data(df):
    tmp = df.dropna(subset=[target_column])
    return tmp

# Filling the null values using linear regression model(X='REF_DATE_INT', Y=null_column).
# Regression model is fit for every province and null column separatly.
# Incase a null column doesn't contain any value for a province then fill Zero.

def fill_na(data, null_columns, feature_columns):
    for dguid in province_dguid:
      df = data[data['DGUID']== dguid]
      for col_name in null_columns:
          linreg = LinearRegression()
          df_with_null = df[feature_columns + [col_name]]
          df_without_null = df_with_null.dropna()
          if(df_without_null[col_name].count() == 0):
            print("******** {} - {} ********".format(df.GEO.iloc[0], col_name))
            df[col_name] = 0
            continue
          x = df_without_null[feature_columns]
          y = df_without_null[col_name]
          linreg.fit(x, y)
          df_with_null['predicted'] = linreg.predict(df_with_null[feature_columns])
          df[col_name].fillna(df_with_null.predicted, inplace=True)
      data.update(df)
    return data

# null_columns - list of columns which contains null values.
# feature_columns - Feature column for regression model.

null_columns = ['income', 'food_expenditures', 'income_taxes', 'mortageinsurance', 'mortagePaid', 'accomodation', 'rent', 'shelter', 'total_expenditure', 'taxes_landregfees', 'international_tourism', 'domestic_tourism', 'employment', 'fulltime', 'labourforce', 'parttime', 'population', 'unemployment', 'employment_rate', 'participationrate', 'unemployment_rate', 'crime_incidents', 'cpi_index', 'diesel_fillingstations', 'diesel_selfservstations', 'premium_fillingstations', 'premium_selfservstations', 'regular_fillingstations', 'regular_selfservstations', 'immigrants', '1y_fixed_posted', '2y_bond', '3y_bond', '3y_fixed_posted', '5y_bond', '5y_fixed_posted', '7y_bond', '10y_bond', 'bank', 'overnight', 'overnight_target', 'prime', 'Mean_Max_Temp', 'Mean_Min_Temp', 'Mean_Temp', 'Total_Rain', 'Total_Snow']
result = fill_na(drop_data(province_data), null_columns, ['REF_DATE_INT'])

print(result.info())

# Splitting the data into train and valid set. 
# Data before year is training and rest is validation/test

def train_valid_split(df, year):
    train = result[result.REF_DATE.map(lambda x: x < year + '-01')]
    valid = result[result.REF_DATE.map(lambda x: x >= year + '-01')]
    return [train, valid]

# Min-Max scaler for normalizing the features.

def min_max_scaler(df):
    scaler = MinMaxScaler()
    df[feature_columns] = scaler.fit_transform(df[feature_columns])
    return df

# Normalizing and splitting the data.

train, valid = train_valid_split(min_max_scaler(result), '2018')
# train, valid = train_valid_split(result, '2018')

print('Training shape:', train.shape)
print('Validation samples: ', valid.shape[0])

# Creating Neural network model for prediction.

def basic_model_3(x_size, y_size):
    t_model = Sequential()
    t_model.add(Dense(80, activation="tanh", kernel_initializer='normal', input_shape=(x_size,)))
    t_model.add(Dropout(0.2))
    t_model.add(Dense(120, activation="relu", kernel_initializer='normal', 
        kernel_regularizer=regularizers.l1(0.01), bias_regularizer=regularizers.l1(0.01)))
    t_model.add(Dropout(0.1))
    t_model.add(Dense(20, activation="relu", kernel_initializer='normal', 
        kernel_regularizer=regularizers.l1_l2(0.01), bias_regularizer=regularizers.l1_l2(0.01)))
    t_model.add(Dropout(0.1))
    t_model.add(Dense(10, activation="relu", kernel_initializer='normal'))
    t_model.add(Dropout(0.0))
    t_model.add(Dense(y_size))
    t_model.compile(
        loss='mean_squared_error',
        optimizer='nadam',
        metrics=[metrics.mae])
    return(t_model)

# Generating the model object and summary.

model = basic_model_3(train[feature_columns].shape[1], pd.DataFrame(train[target_column]).shape[1])
model.summary()

epochs = 1000
batch_size = 64

print('Epochs: ', epochs)
print('Batch size: ', batch_size)

# Training the neural network model

history = model.fit(train[feature_columns], pd.DataFrame(train[target_column]),
    batch_size=batch_size,
    epochs=epochs,
    shuffle=True,
    verbose=1, # Change it to 2, if wished to observe execution
    validation_data=(valid[feature_columns], pd.DataFrame(valid[target_column])))

# Print training and validation score
# Save the model and prediction

train_score = model.evaluate(train[feature_columns], pd.DataFrame(train[target_column]), verbose=0)
valid_score = model.evaluate(valid[feature_columns], pd.DataFrame(valid[target_column]), verbose=0)

print('Train MAE: ', round(train_score[1], 4), ', Train Loss: ', round(train_score[0], 4)) 
print('Val MAE: ', round(valid_score[1], 4), ', Val Loss: ', round(valid_score[0], 4))

valid['predict'] = model.predict(valid[feature_columns])

result['predicted_total_house_index'] = model.predict(min_max_scaler(result[feature_columns]))

model.save('Output/neural_network_model.h5')
result.to_csv("Output/data_nn.csv", header='true')

# Produce a plot for the validation/test data for neural network model.

for dguid in province_dguid:
  data = valid[valid['DGUID'] == dguid]
  plt.plot(np.array(data['REF_DATE']), data[target_column])
  plt.plot(np.array(data['REF_DATE']), data['predict'])
  plt.ylabel('Total Housing Index')
  plt.legend(['Actual Index','Predicted Index'])
  plt.title('Neural Network Model - {}'.format(data.GEO.iloc[0]))
  plt.xlabel('Time')
  plt.xticks(rotation=45)
  plt.savefig('Output/plot/NN_{}.png'.format(data.GEO.iloc[0]))
  plt.show()

# Linear Regression Model for housing index prediction
# Pipeline contains the dimensionality reduction, preprocessing and ridge model

from sklearn.feature_selection import SelectKBest, f_regression
from sklearn.linear_model import Ridge
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline

def regression_model():
  model = make_pipeline(SelectKBest(f_regression, k=15), PolynomialFeatures(3), Ridge(alpha=0.01))
  model.fit(train[feature_columns], pd.DataFrame(train[target_column]))
  return model

reg_model = regression_model()
print('Train Score: ', round(reg_model.score(train[feature_columns], pd.DataFrame(train[target_column])), 4))
print('Valid Score: ', round(reg_model.score(valid[feature_columns], pd.DataFrame(valid[target_column])), 4))

valid['predict'] = reg_model.predict(valid[feature_columns])
result['predicted_total_house_index'] = reg_model.predict(min_max_scaler(result[feature_columns]))

# Saving the model and the file
pkl_filename = "Output/regression_model.pkl"
with open(pkl_filename, 'wb') as file:  
    pickle.dump(reg_model, file)
result.to_csv("Output/data_regression.csv", header='true')

# Produce a plot for the results.
for dguid in province_dguid:
  data = valid[valid['DGUID'] == dguid]
  plt.plot(np.array(data['REF_DATE']), data[target_column])
  plt.plot(np.array(data['REF_DATE']), data['predict'])
  plt.ylabel('Total Housing Index')
  plt.legend(['Actual Index','Predicted Index'])
  plt.title('Linear Regression - {}'.format(data.GEO.iloc[0]))
  plt.xlabel('Time')
  plt.xticks(rotation=45)
  plt.savefig('Output/plot/Regression_{}.png'.format(data.GEO.iloc[0]))
  plt.show()



