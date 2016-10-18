from flask import Flask, render_template, request, jsonify
from flask_bootstrap import Bootstrap
import pymongo
import pandas as pd
import numpy as np

app = Flask(__name__)
Bootstrap(app)

MODELS = ['vader', 'textblob']

# home page
@app.route('/')
def index():
    return render_template('index.html', models=MODELS)

@app.route('/get_threads_for_model')
def get_threads_for_model():
    mc = pymongo.MongoClient()
    return jsonify(threads = [i for i in mc['reddit'][request.args.get('model')].find()])

@app.route('/get_plot')
def get_plot():
    mc = pymongo.MongoClient()
    model = request.args.get('model')
    thread = request.args.get('thread')
    df = pd.DataFrame([i for i in mc['reddit']['%s_%s' % (model, thread)].find(sort=[('count', -1)])])
    return jsonify(info = {i[0]:list(i[1]) for i in zip(df.columns, np.asarray(df.transpose()))})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
