const template = document.querySelector("#listing-template");
const currentPath = window.location.pathname;
const archiveMatch = currentPath.match(/\/(\d{4}-\d{2}-\d{2})\/?(?:index\.html)?$/);
const rootPrefix = archiveMatch ? "../" : "./";

const fmtNumber = (value) =>
  value == null ? "n/a" : new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value);

const createStat = (label, value) => {
  const wrap = document.createElement("div");
  wrap.className = "stat";

  const labelEl = document.createElement("span");
  labelEl.className = "stat-label";
  labelEl.textContent = label;

  const valueEl = document.createElement("span");
  valueEl.className = "stat-value";
  valueEl.textContent = value;

  wrap.append(labelEl, valueEl);
  return wrap;
};

const renderListing = (record, index) => {
  const node = template.content.firstElementChild.cloneNode(true);
  node.querySelector(".rank-badge").textContent = `#${index + 1}`;

  const strict = node.querySelector(".strict-badge");
  strict.textContent = record.criteria_notes?.some((note) => note.includes("outside target budget")) ||
    record.criteria_notes?.some((note) => note.includes("misses target"))
    ? "Near Miss"
    : "Strict Match";
  if (strict.textContent === "Near Miss") strict.classList.add("near-miss");

  node.querySelector(".listing-title").textContent = record.property_name || record.title || "Untitled listing";
  node.querySelector(".listing-subtitle").textContent = record.address || record.access_text || "No address";

  const stats = node.querySelector(".stats-grid");
  stats.append(
    createStat("Price", `${fmtNumber(record.price_man)}万円`),
    createStat("Size", `${fmtNumber(record.area_sqm)} sqm`),
    createStat("Layout", record.layout || "n/a"),
    createStat("Walk", record.walk_min == null ? "n/a" : `${record.walk_min} min`),
    createStat("Built", record.built_year ?? "n/a"),
    createStat("Stations", (record.exact_station_hits || []).join(", ") || (record.nearby_station_hits || []).join(", ") || "n/a"),
  );

  if (record.land_area_sqm != null) {
    stats.append(createStat("Land", `${fmtNumber(record.land_area_sqm)} sqm`));
  }

  node.querySelector(".summary").textContent = record.detail_summary || "No summary available.";

  const tags = node.querySelector(".tag-row");
  const pills = [
    ...(record.dishwasher_hits?.length ? ["Dishwasher"] : []),
    ...(record.brightness_hits || []).slice(0, 3),
    ...(record.ceiling_hits || []).slice(0, 2),
  ];
  for (const pill of pills) {
    const el = document.createElement("span");
    el.className = "pill";
    el.textContent = pill;
    tags.append(el);
  }

  const notesList = node.querySelector(".notes-list");
  for (const note of record.criteria_notes || []) {
    const li = document.createElement("li");
    li.textContent = note;
    notesList.append(li);
  }

  const link = node.querySelector(".listing-link");
  link.href = record.url;
  return node;
};

const renderList = (containerId, rows) => {
  const container = document.getElementById(containerId);
  container.replaceChildren(...rows.map(renderListing));
};

const renderArchivePicker = (site) => {
  const select = document.getElementById("archive-select");
  const viewing = document.getElementById("viewing-label");
  const archiveNote = document.getElementById("archive-note");
  const currentRun = site.current_run_date || "latest";
  const viewingText = site.is_latest ? `Latest (${currentRun})` : `Archive ${currentRun}`;

  viewing.textContent = viewingText;
  archiveNote.textContent = site.is_latest
    ? "Archive snapshots are organized by run date in YYYY-MM-DD format."
    : `This page is an archived snapshot for ${currentRun}.`;

  for (const runDate of site.archives || []) {
    const option = document.createElement("option");
    option.value = runDate;
    option.textContent = runDate;
    if (runDate === currentRun && !site.is_latest) {
      option.selected = true;
    }
    select.append(option);
  }

  select.addEventListener("change", () => {
    const runDate = select.value;
    window.location.href = runDate ? `${rootPrefix}${runDate}/` : `${rootPrefix}`;
  });
};

const load = async () => {
  const [site, mansions, houses] = await Promise.all([
    fetch("./data/site.json").then((r) => r.json()),
    fetch("./data/mansions.json").then((r) => r.json()),
    fetch("./data/houses.json").then((r) => r.json()),
  ]);

  document.getElementById("generated-at").textContent = new Date(site.generated_at).toLocaleString();
  document.getElementById("mansion-count").textContent = fmtNumber(site.mansion_count);
  document.getElementById("house-count").textContent = fmtNumber(site.house_count);
  renderArchivePicker(site);

  renderList("mansion-list", mansions);
  renderList("house-list", houses);
};

load().catch((error) => {
  console.error(error);
  document.getElementById("generated-at").textContent = "Failed to load site data";
});
