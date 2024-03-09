import json
from flask import Flask, request, render_template

from postgres.database import init_db
from recommendations.recommender import Recommender

app = Flask(__name__)

init_db()
RECOMMENDER = Recommender()


@app.route('/')
def index():
    return render_template('recommendation_form.html')


@app.route('/recommend', methods=['GET'])
def recommend():
    user_id = request.args.get('user_id')
    if not user_id:
        return "User ID is required", 400

    recommendations, recommendations_desc = RECOMMENDER.get_recommendations(user_id)
    return json.dumps(recommendations_desc), 200


if __name__ == '__main__':
    app.run(debug=True)
