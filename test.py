from tatami import Client

client = Client('http://0.0.0.0:3000')
client.load_dataset('test_s3')

print('hoge')
