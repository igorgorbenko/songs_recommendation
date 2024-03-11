import json
from flask import Flask, request, render_template, jsonify

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

    recommendations, recommendations_desc, user_vector = RECOMMENDER.get_recommendations(user_id)
    recommendations_with_names = [{'id': id, 'name': name} for id, name in zip(recommendations, recommendations_desc)]
    return jsonify({'recommendations': recommendations_with_names, 'user_vector': user_vector})


@app.route('/impression', methods=['POST'])
def impression():
    data = request.json
    user_id = data.get('user_id')
    song_id = data.get('song_id')
    is_like = bool(data.get('is_like'))

    if not user_id:
        return "User ID is required", 400

    if not song_id:
        return "Song ID is required", 400

    result = RECOMMENDER.put_impression(user_id, song_id, is_like)
    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True)
