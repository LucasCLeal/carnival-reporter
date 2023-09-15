import faust

app = faust.App('hello-app', broker='kafka://localhost:9092',value_serializer='raw',)
topic = app.topic('beta_input_topic')

@app.agent(topic)
async def hello(messages):
    async for message in messages:
        print(message)

if __name__ == '__main__':
    app.main()