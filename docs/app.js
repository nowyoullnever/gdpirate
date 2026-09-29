const button = document.querySelector("#lucky-button");
const result = document.querySelector("#result");

async function getRandomDriveLink() {
  // UI v1 placeholder.
  // Replace with the GDPirate /api/random backend in the backend-integration task.
  return {
    url: "https://drive.google.com/",
    source: {
      name: "placeholder",
      url: "https://drive.google.com/"
    }
  };
}

function renderResult(payload) {
  result.replaceChildren();

  if (!payload || !payload.url) {
    result.textContent = "No link available.";
    return;
  }

  const link = document.createElement("a");
  link.href = payload.url;
  link.target = "_blank";
  link.rel = "noopener noreferrer";
  link.textContent = payload.url;
  result.replaceChildren(link);
}

button.addEventListener("click", async () => {
  button.disabled = true;
  try {
    renderResult(await getRandomDriveLink());
  } catch {
    renderResult(null);
  } finally {
    button.disabled = false;
  }
});
