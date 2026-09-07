const form = document.querySelector("#import-form");
const identity = document.querySelector("#identity");
const records = document.querySelector("#records");
const statusLine = document.querySelector("#form-status");
const jobsNode = document.querySelector("#jobs");
const emptyState = document.querySelector("#empty-state");
const submitButton = document.querySelector("#submit-button");

function headers(extra = {}) {
  return { Authorization: `Bearer ${identity.value}`, ...extra };
}

function escapeHtml(value) {
  const node = document.createElement("span");
  node.textContent = value;
  return node.innerHTML;
}

async function parseResponse(response) {
  const body = await response.json();
  if (!response.ok) {
    const detail = typeof body.detail === "string" ? body.detail : "The request was rejected.";
    throw new Error(detail);
  }
  return body;
}

async function loadJobs() {
  try {
    const response = await fetch("/jobs", { headers: headers() });
    const jobs = await parseResponse(response);
    emptyState.hidden = jobs.length > 0;
    jobsNode.innerHTML = jobs.map((job) => {
      const result = job.result
        ? `<div class="job-result">${job.result.accepted_count} accepted · sum ${job.result.value_sum}</div>`
        : job.error ? `<div class="job-result">${escapeHtml(job.error.code)}</div>` : "";
      return `<article class="job">
        <div><h3>${escapeHtml(job.id.slice(0, 8))}…</h3>
          <div class="job-meta">${job.record_count} records · attempt ${job.attempt}/${job.max_attempts} · revision ${job.revision}</div>
          ${result}
        </div>
        <span class="badge ${escapeHtml(job.state)}">${escapeHtml(job.state.replace("_", " "))}</span>
      </article>`;
    }).join("");
  } catch (error) {
    statusLine.textContent = error.message;
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const values = records.value.split(",").map((item) => item.trim()).filter(Boolean);
  if (!values.length || values.some((item) => !/^-?\d+$/.test(item))) {
    statusLine.textContent = "Enter comma-separated whole numbers.";
    return;
  }
  submitButton.disabled = true;
  statusLine.textContent = "Persisting request…";
  try {
    const payload = { kind: "data_import", records: values.map((value) => ({ value: Number(value) })) };
    const response = await fetch("/jobs", {
      method: "POST",
      headers: headers({ "Content-Type": "application/json", "Idempotency-Key": crypto.randomUUID() }),
      body: JSON.stringify(payload),
    });
    await parseResponse(response);
    statusLine.textContent = "Request accepted and queued.";
    await loadJobs();
  } catch (error) {
    statusLine.textContent = error.message;
  } finally {
    submitButton.disabled = false;
  }
});

identity.addEventListener("change", loadJobs);
document.querySelector("#refresh").addEventListener("click", loadJobs);
loadJobs();
setInterval(loadJobs, 1500);

