const current = document.body.dataset.page;

document.querySelectorAll("nav a").forEach((link) => {
  const href = link.getAttribute("href") || "";
  if (
    (current === "home" && href.endsWith("index.html")) ||
    (current !== "home" && href.includes(`${current}.html`))
  ) {
    link.classList.add("is-active");
  }
});

document.querySelectorAll("[data-year]").forEach((node) => {
  node.textContent = String(new Date().getFullYear());
});

