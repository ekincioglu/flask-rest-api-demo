import pandas as pd
from flask import Flask, render_template, request
from flask_restful import Resource, Api, reqparse
import json
from datetime import datetime
import ast

app = Flask(__name__)
api = Api(app)

def exportAsJSON(dataframe):
    with open('data.json', 'w') as f:
        json.dump(json.loads(dataframe.to_json(orient="split")), f)
        
def getJSON(dataframe):
    return json.dumps(json.loads(dataframe.to_json(orient="split")), indent=4)

# dummy apiTimeseries
ts1 = open('ts1.json'); ts1_data = json.load(ts1)
ts2 = open('ts2.json'); ts2_data = json.load(ts2)
ts3 = open('ts3.json'); ts3_data = json.load(ts3)
ts4 = open('ts4.json'); ts4_data = json.load(ts4)

# create dataframes
mf1 = pd.DataFrame(data={'timestamps': pd.to_datetime(ts1_data['timestamps']), ts1_data['timeSeriesId']: ts1_data['values']})
mf2 = pd.DataFrame(data={'timestamps': pd.to_datetime(ts2_data['timestamps']), ts2_data['timeSeriesId']: ts2_data['values']})
mf3 = pd.DataFrame(data={'timestamps': pd.to_datetime(ts3_data['timestamps']), ts3_data['timeSeriesId']: ts3_data['values']})
mf4 = pd.DataFrame(data={'timestamps': pd.to_datetime(ts4_data['timestamps']), ts4_data['timeSeriesId']: ts4_data['values']})

# merge them all together
merge1 = pd.merge(mf1,mf2, on='timestamps', how='outer')
merge2 = pd.merge(merge1,mf3, on='timestamps', how='outer')
merge3 = pd.merge(merge2,mf4, on='timestamps', how='outer')

# sort and drop index column from dataframe
sortedDataFrame = merge3.sort_values(by=['timestamps']).set_index(merge3['timestamps'])
#print(sortedDataFrame.head())
############### API ########################

df = sortedDataFrame.resample((str(30) + 'Min'), on='timestamps').mean()
print(df.to_string())

class Users(Resource):
    def get(self):
        username = request.args.get('username')
        password = request.args.get('password')
        interval = request.args.get('interval')
        kind = request.args.get('kind')

        if kind == 'max':
          agg_dataframe = sortedDataFrame.resample((str(interval) + 'Min'), on='timestamps').max()
        elif kind == 'min':
          agg_dataframe = sortedDataFrame.resample((str(interval) + 'Min'), on='timestamps').min()
        elif kind == 'mean':
          agg_dataframe = sortedDataFrame.resample((str(interval) + 'Min'), on='timestamps').mean()
        else:
          agg_dataframe = sortedDataFrame.resample((str(interval) + 'Min'), on='timestamps').mean()

        if(username != 'melih' or password != '1234'):
            return {'message': '401 Unauthorized'}

        data = json.loads(getJSON(agg_dataframe))
        # DATE = [datetime.fromtimestamp(x) for x in data['index']]
        return {'timeSeriesIds': data['columns'], 'timestamps': data['index'], 'values': data['data']}, 200
    #methods go here
    pass
    
class Fill(Resource):
    def get(self):
        username = request.args.get('username')
        password = request.args.get('password')
        if(username != 'melih' or password != '1234'):
            return {'message': '401 Unauthorized'}
        dataframe = sortedDataFrame.bfill().ffill()
        data = json.loads(getJSON(dataframe))
        return data, 200
    pass
    
api.add_resource(Users, '/aggregate')
api.add_resource(Fill, '/fill')

############### API ########################





############### WEB SITE ###################

#headings = dataframe.columns.tolist()
#data = dataframe.values.tolist()

#headings2 = dataframe2.columns.tolist()
#data2 = dataframe2.values.tolist()

@app.route('/')
def index():
    return render_template('index.html')

# @app.route('/fill')
# def fill():
    # return render_template('fill.html', headings=headings, data=data )

# @app.route('/resample')    
# def resample():
    # return render_template('resample.html', headings=headings2, data=data2)
    
############### WEB SITE ###################



    
if __name__ == '__main__':
    app.run(debug=True)