from flask import Flask, request, render_template, jsonify
from recommendations.recommender import Recommender

app = Flask(__name__)
RECOMMENDER = Recommender()


@app.route('/')
def index():
    return render_template('recommendation_form.html')


@app.route('/recommend', methods=['GET'])
def recommend():
    user_id = request.args.get('user_id')
    if not user_id:
        return "User ID is required", 400

    request_id, recommendations, user_vector = RECOMMENDER.get_recommendations(user_id)
    return jsonify({'request_id': request_id,
                    'recommendations': recommendations,
                    'user_vector': user_vector})


@app.route('/impression', methods=['POST'])
def impression():
    data = request.json
    request_id = data.get('request_id')
    user_id = data.get('user_id')
    song_id = data.get('song_id')
    artist = data.get('artist')
    is_like = data.get('is_like')

    if not user_id:
        return "User ID is required", 400
    if not song_id:
        return "Song ID is required", 400
    if artist is None:
        return "Artist is required", 400
    if is_like is None:
        return "Impression (is_like) is required", 400

    result = RECOMMENDER.put_impression(request_id, user_id, song_id, artist, is_like)
    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True)
