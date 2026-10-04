import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 50 },
    { duration: '90s', target: 200 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<250'],
  },
};

const BASE = 'http://broker:8080';
const regions = ['asia-south', 'eu-west', 'us-east'];
const headers = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const region = regions[Math.floor(Math.random() * regions.length)];
  const body = JSON.stringify({ user_id: `vu${__VU}-${__ITER}`, region: region });

  const created = http.post(`${BASE}/sessions`, body, headers);
  check(created, { 'created (201 or 202)': (r) => r.status === 201 || r.status === 202 });
  if (created.status !== 201 && created.status !== 202) {
    sleep(0.1);
    return;
  }
  const id = created.json('id');

  const got = http.get(`${BASE}/sessions/${id}`);
  check(got, { 'read 200': (r) => r.status === 200 });

  const released = http.del(`${BASE}/sessions/${id}`);
  check(released, { 'released 200': (r) => r.status === 200 });

  sleep(0.05);
}