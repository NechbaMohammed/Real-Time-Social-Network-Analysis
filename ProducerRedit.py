from kafka import KafkaProducer
import json
import praw
import time



# Set up your Reddit API credentials
reddit_client_id = 'g7tD8jb5uxaxsERDWMTWRw'
reddit_client_secret = '57mp2urI2lazy6MQb3ptp18i3aAevA'
reddit_user_agent = 'MALIKI Ayoub'
username="Dense-Special-2992"
password="Ayoubmalikisp1"

# Set up your Kafka producer
bootstrap_servers = 'localhost:9092'
topic = 'extract_data_topic'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                        
# Initialize the Reddit API wrapper
reddit = praw.Reddit(
    client_id=reddit_client_id,
    client_secret=reddit_client_secret,
    user_agent=reddit_user_agent,
    username = username,
    password = password
)

class RedditStreamListener:
    def on_submission(self, submission):
    # This method is called whenever a new Reddit submission is received
        if submission.author is not None:
            reddit_post = {
                'text': submission.title,
                'score': submission.score,
                'url': submission.url,
                'author': submission.author.name,
                'created_utc': submission.created_utc,
            }
            producer.send(topic, value=reddit_post)
            print("Reddit post sent to Kafka")
        else:
            print("Submission has no author. Skipping...")


    def on_error(self, status_code):
        print(f"Error with status code: {status_code}")
        return True

# Set up Reddit stream listener
reddit_stream_listener = RedditStreamListener()

subreddit_name = 'AskMiddleEast'
subreddit = reddit.subreddit(subreddit_name)

while True:
    for submission in subreddit.new(limit=None):
        reddit_stream_listener.on_submission(submission)

    # Wait for 60 seconds before checking for new submissions again
    time.sleep(60)

# Close the Kafka producer when done
producer.close()