const output = document.getElementById("output");

document.getElementById("open-ui").addEventListener("click", () => {
  window.open("http://127.0.0.1:11435/ui", "_blank");
});

document.getElementById("open-health").addEventListener("click", async () => {
  output.textContent = "Checking...";
  try {
    const res = await fetch("http://127.0.0.1:11435/health");
    const json = await res.json();
    output.textContent = JSON.stringify(json, null, 2);
  } catch (err) {
    output.textContent = String(err);
  }
});
