- Different microservices need sharing of data, but can't just have that many API calls
- Event based architecture
- Event records something, has a key, value, timestamp, and some headers

```
docker compose down
docker compose down --remove-orphans
docker ps
docker compose up -d
```

curl -X POST http://localhost:3000/api/v1/comments \
 -H "Content-Type: application/json" \
 -d '{"text":"hello kafka"}'
