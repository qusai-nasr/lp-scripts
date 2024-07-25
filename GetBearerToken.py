import requests

# Documentation: https://developers.liveperson.com/third-party-bots-public-api.html#step-1-identify-the-third-party-bots-api-domain

url = "https://bot-platform-api.emea.fs.liveperson.com/api/v1/account/52375911/login?v=1.3"
payload = {
    "authType": "USER_PASS",
    "credentials": {
        "username": "someuser",
        "password": "123456"
    }
}

response = requests.post(url, json=payload)

if response.status_code == 200:
    data = response.json()
    bearer_token = data.get("bearer")
    print("Bearer Token:", bearer_token)
else:
    print("Failed to get bearer token, status code:", response.status_code)