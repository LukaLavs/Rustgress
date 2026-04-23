const http = require("http");

const PORT = 3000;

const html = `
<!DOCTYPE html>
<html lang="sl">
<head>
    <meta charset="UTF-8">
    <title>RustgreSQL | Pro Editorial</title>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600&family=JetBrains+Mono&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #f8f9fa;
            --panel: #ffffff;
            --accent: #000000;
            --text: #1a1a1a;
            --text-dim: #707070;
            --border: #e1e1e1;
            --subtle-bg: #f1f1f1;
            --editor-bg: #ffffff;
            --history-card: #f1f1f1;
            --table-hover: #f9f9f9;
        }

        [data-theme="dark"] {
            --bg: #0a0a0a;
            --panel: #111111;
            --accent: #ffffff;
            --text: #eeeeee;
            --text-dim: #888888;
            --border: #222222;
            --subtle-bg: #1a1a1a;
            --editor-bg: #0f0f0f;
            --history-card: #1a1a1a;
            --table-hover: #161616;
        }

        body { 
            background: var(--bg); 
            color: var(--text); 
            font-family: 'Plus Jakarta Sans', sans-serif;
            margin: 0;
            display: flex;
            height: 100vh;
            overflow: hidden;
            transition: background 0.3s ease;
        }

        /* Sidebar */
        .sidebar {
            width: 300px;
            background: var(--panel);
            border-right: 1px solid var(--border);
            display: flex;
            flex-direction: column;
            padding: 32px 24px;
            transition: all 0.3s ease;
        }

        .branding { margin-bottom: 48px; }
        .branding h1 { font-size: 1.2rem; font-weight: 600; margin: 0; letter-spacing: -0.5px; color: var(--accent); }
        .branding p { font-size: 0.65rem; color: var(--text-dim); margin: 4px 0 0 0; text-transform: uppercase; letter-spacing: 0.8px; line-height: 1.4; }

        .sidebar-title { font-size: 0.65rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; color: var(--text-dim); margin-bottom: 16px; }

        #history { flex: 1; overflow-y: auto; padding-right: 4px; }
        .history-card { background: var(--history-card); border-radius: 12px; padding: 14px; margin-bottom: 12px; border: 1px solid transparent; }
        .history-code { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--text); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; display: block; margin-bottom: 10px; opacity: 0.9; }
        .history-actions { display: flex; gap: 12px; }
        .action-link { font-size: 10px; font-weight: 700; text-decoration: none; color: var(--text-dim); cursor: pointer; text-transform: uppercase; letter-spacing: 0.5px; }
        .action-link:hover { color: var(--accent); }

        .theme-toggle {
            margin-top: 20px;
            font-size: 10px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-dim);
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px;
            border-radius: 8px;
            background: var(--subtle-bg);
            transition: all 0.2s;
        }
        .theme-toggle:hover { background: var(--border); color: var(--accent); }

        /* Main Area */
        .main-content {
            flex: 1;
            display: flex;
            flex-direction: column;
            padding: 40px 60px;
            background: var(--editor-bg);
            overflow: hidden; /* Glavni kontejner ne scrolla, scrollajo poddeli */
        }

        .tabs-row { display: flex; align-items: center; gap: 24px; margin-bottom: 24px; flex-shrink: 0; }
        .tab { font-size: 13px; font-weight: 500; color: var(--text-dim); cursor: pointer; padding: 8px 0; border-bottom: 2px solid transparent; transition: all 0.2s; white-space: nowrap; }
        .tab.active { color: var(--accent); border-bottom-color: var(--accent); }
        .btn-new-tab { background: none; border: none; font-size: 20px; color: var(--text-dim); cursor: pointer; line-height: 1; transition: color 0.2s; }
        .btn-new-tab:hover { color: var(--accent); }

        /* Editor */
        .editor-section { flex-shrink: 0; margin-bottom: 32px; }
        .editor-box { border: 1px solid var(--border); border-radius: 16px; padding: 24px; background: var(--panel); transition: all 0.3s; }
        .editor-box:focus-within { border-color: var(--accent); box-shadow: 0 0 0 4px rgba(0,0,0,0.02); }
        #editor { width: 100%; min-height: 80px; max-height: 200px; overflow-y: auto; font-family: 'JetBrains Mono', monospace; font-size: 15px; line-height: 1.6; outline: none; color: var(--text); }
        .footer-hint { font-size: 10px; color: var(--text-dim); margin-top: 14px; display: flex; justify-content: space-between; text-transform: uppercase; letter-spacing: 0.5px; }

        /* Result Area (PRO Tables) */
        .result-section { 
            flex: 1; 
            overflow: hidden; 
            display: flex; 
            flex-direction: column; 
            border-top: 1px solid var(--border);
            padding-top: 20px;
        }
        
        .table-viewport {
            flex: 1;
            overflow: auto; /* Tu se zgodi magija za velike tabele */
            border-radius: 8px;
            background: var(--panel);
        }

        table { border-collapse: collapse; width: 100%; table-layout: auto; }
        
        thead th { 
            position: sticky; 
            top: 0; 
            background: var(--panel); 
            z-index: 2;
            text-align: left; 
            font-size: 11px; 
            text-transform: uppercase; 
            letter-spacing: 1px; 
            color: var(--text-dim); 
            padding: 16px 20px; 
            border-bottom: 2px solid var(--border); 
            font-weight: 600;
            white-space: nowrap;
        }

        tbody td { 
            padding: 14px 20px; 
            font-size: 13px; 
            border-bottom: 1px solid var(--border); 
            color: var(--text); 
            white-space: nowrap; /* Prepreči lomljenje pri 50 stolpcih */
            font-family: 'JetBrains Mono', monospace;
        }

        tbody tr:hover { background: var(--table-hover); }

        /* Custom Scrollbar */
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 10px; }
        ::-webkit-scrollbar-thumb:hover { background: var(--text-dim); }

        /* Toast */
        .toast { position: fixed; bottom: 32px; right: 32px; background: var(--accent); color: var(--panel); padding: 12px 24px; border-radius: 12px; font-size: 12px; font-weight: 600; display: none; z-index: 1000; box-shadow: 0 10px 30px rgba(0,0,0,0.2); }
    </style>
</head>
<body data-theme="light">

<div class="sidebar">
    <div class="branding">
        <h1>RustgreSQL</h1>
        <p>Minimal SQL Database<br>Inspired by PostgreSQL</p>
    </div>

    <div class="sidebar-title">History</div>
    <div id="history"></div>

    <div class="theme-toggle" onclick="toggleTheme()">
        <span id="theme-icon">☾</span> <span id="theme-text">Night Mode</span>
    </div>
</div>

<div class="main-content">
    <div class="tabs-row">
        <div id="tabs-list" style="display: flex; gap: 24px;"></div>
        <button class="btn-new-tab" onclick="addNewTab()" title="New Tab">+</button>
    </div>

    <div class="editor-section">
        <div class="editor-box">
            <div id="editor" contenteditable="true" spellcheck="false"></div>
            <div class="footer-hint">
                <span><b>Enter</b> Execute &nbsp;•&nbsp; <b>Shift+Enter</b> New Line</span>
                <span id="char-count">0 characters</span>
            </div>
        </div>
    </div>

    <div class="result-section">
        <div class="table-viewport" id="result-scroll-container">
            <div id="result"></div>
        </div>
    </div>
</div>

<div id="toast" class="toast">Copied to clipboard</div>

<script>
    let tabs = [{query: "SELECT * FROM USERS", result: null}];
    let currentTabIdx = 0;
    let historyData = [];

    const editor = document.getElementById("editor");

    function init() {
        renderTabs();
        loadCurrentTab();
    }

    function toggleTheme() {
        const body = document.body;
        const currentTheme = body.getAttribute("data-theme");
        const newTheme = currentTheme === "light" ? "dark" : "light";
        body.setAttribute("data-theme", newTheme);
        document.getElementById("theme-text").innerText = newTheme === "light" ? "Night Mode" : "Day Mode";
        document.getElementById("theme-icon").innerText = newTheme === "light" ? "☾" : "☼";
    }

    function renderTabs() {
        const container = document.getElementById("tabs-list");
        container.innerHTML = tabs.map((t, i) => \`
            <div class="tab \${i === currentTabIdx ? 'active' : ''}" onclick="switchTab(\${i})">
                Query \${i + 1}
            </div>
        \`).join("");
    }

    function switchTab(idx) {
        tabs[currentTabIdx].query = editor.innerText;
        currentTabIdx = idx;
        renderTabs();
        loadCurrentTab();
    }

    function addNewTab() {
        tabs[currentTabIdx].query = editor.innerText;
        tabs.push({query: "", result: null});
        currentTabIdx = tabs.length - 1;
        renderTabs();
        loadCurrentTab();
    }

    function loadCurrentTab() {
        editor.innerText = tabs[currentTabIdx].query;
        document.getElementById("result").innerHTML = tabs[currentTabIdx].result || 
            '<div style="padding:40px; color:var(--text-dim); font-size:13px; text-align:center;">Execute a query to see results</div>';
        updateCharCount();
    }

    async function run() {
        let query = editor.innerText.trim();
        if (!query) return;

        document.getElementById("result").innerHTML = '<div style="padding:40px; color:var(--accent); font-size:13px;">Executing...</div>';

        try {
            let res = await fetch("/query", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({query})
            });
            let data = await res.json();
            
            let tableHtml = renderTable(data);
            tabs[currentTabIdx].result = tableHtml;
            document.getElementById("result").innerHTML = tableHtml;
            
            addToHistory(query);
        } catch (e) {
            document.getElementById("result").innerHTML = '<div style="padding:40px; color:red;">Network Error: Backend not reachable</div>';
        }
    }

    function renderTable(data) {
        if (!data.columns || data.columns.length === 0) return '<div style="padding:20px;">No data returned.</div>';
        
        let html = "<table><thead><tr>" + data.columns.map(c => \`<th>\${c}</th>\`).join("") + "</tr></thead><tbody>";
        data.rows.forEach(r => {
            html += "<tr>" + r.map(cell => \`<td>\${cell}</td>\`).join("") + "</tr>";
        });
        return html + "</tbody></table>";
    }

    function addToHistory(q) {
        if (historyData[0] === q) return;
        historyData.unshift(q);
        renderHistory();
    }

    function renderHistory() {
        document.getElementById("history").innerHTML = historyData.slice(0, 15).map((q, i) => \`
            <div class="history-card">
                <span class="history-code">\${q}</span>
                <div class="history-actions">
                    <span class="action-link" onclick="restoreQuery(\${i})">Restore</span>
                    <span class="action-link" onclick="copyToClipboard(\${i})">Copy</span>
                </div>
            </div>
        \`).join("");
    }

    function restoreQuery(i) {
        editor.innerText = historyData[i];
        updateCharCount();
        editor.focus();
    }

    function copyToClipboard(i) {
        navigator.clipboard.writeText(historyData[i]);
        const t = document.getElementById("toast");
        t.style.display = "block";
        setTimeout(() => { t.style.display = "none"; }, 2000);
    }

    function updateCharCount() {
        document.getElementById("char-count").innerText = editor.innerText.length + " characters";
    }

    editor.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            run();
        }
    });

    editor.addEventListener("input", updateCharCount);

    init();
</script>
</body>
</html>
`;

// ===== FAKE DATABASE (DEMO) =====
function fakeDB(query) {
    const q = query.toUpperCase();
    
    if (q.includes("BIG")) {
        const cols = Array.from({length: 50}, (_, i) => `COL_\${i + 1}`);
        const rows = Array.from({length: 50}, (_, r) => 
            Array.from({length: 50}, (_, c) => `DATA_\${r+1}:\${c+1}`)
        );
        return { columns: cols, rows: rows };
    }

    if (q.includes("USERS")) {
        return { 
            columns: [
                "ID",
                "USERNAME",
                "ROLE",
                "EMAIL",
                "LAST_IP",
                "CREATED_AT",
                "LAST_LOGIN",
                "STATUS",
                "LOGIN_COUNT",
                "ORG",
                "PLAN",
                "2FA_ENABLED"
            ], 
            rows: [
                [1, "admin", "Owner", "admin@system.local", "127.0.0.1", "2024-01-01", "2026-04-21", "Online", 9999, "RootOrg", "Enterprise", true],
                [2, "janez_rust", "Developer", "janez@rust.dev", "192.168.1.15", "2024-02-15", "2026-04-20", "Offline", 321, "DevTeam", "Pro", true],
                [3, "eva_sql", "Analyst", "eva@data.ai", "10.0.0.5", "2024-03-10", "2026-04-22", "Online", 512, "Analytics", "Pro", true],
                [4, "mario_db", "DBA", "mario@db.io", "10.1.1.2", "2024-04-01", "2026-04-18", "Online", 1200, "Infra", "Enterprise", true],
                [5, "luka_dev", "Developer", "luka@code.rs", "172.16.0.9", "2024-04-10", "2026-04-19", "Offline", 88, "DevTeam", "Free", false],
                [6, "tina_ui", "Designer", "tina@ui.design", "192.168.0.25", "2024-05-01", "2026-04-21", "Online", 230, "Design", "Pro", true],
                [7, "marko_ops", "DevOps", "marko@ops.net", "10.0.0.8", "2024-05-15", "2026-04-22", "Online", 1500, "Infra", "Enterprise", true],
                [8, "sara_ml", "Data Scientist", "sara@ai.ml", "10.2.3.4", "2024-06-01", "2026-04-20", "Offline", 640, "AI", "Pro", true],
                [9, "filip_web", "Developer", "filip@web.dev", "192.168.1.44", "2024-06-10", "2026-04-17", "Online", 410, "DevTeam", "Free", false],
                [10, "ana_sec", "Security", "ana@sec.io", "10.0.0.11", "2024-06-15", "2026-04-22", "Online", 980, "Security", "Enterprise", true],
                [11, "jure_sql", "Analyst", "jure@sql.ai", "192.168.2.10", "2024-07-01", "2026-04-16", "Offline", 150, "Analytics", "Free", false],
                [12, "petra_pm", "Manager", "petra@pm.org", "10.0.1.5", "2024-07-10", "2026-04-21", "Online", 670, "Management", "Pro", true],
                [13, "igor_backend", "Developer", "igor@backend.rs", "172.16.1.20", "2024-07-20", "2026-04-22", "Online", 890, "DevTeam", "Pro", true],
                [14, "nina_front", "Developer", "nina@frontend.dev", "192.168.1.60", "2024-08-01", "2026-04-19", "Offline", 300, "DevTeam", "Free", false],
                [15, "klemen_db", "DBA", "klemen@db.rs", "10.0.2.2", "2024-08-10", "2026-04-18", "Online", 1100, "Infra", "Enterprise", true],
                [16, "zala_data", "Analyst", "zala@data.org", "192.168.0.90", "2024-08-20", "2026-04-22", "Online", 520, "Analytics", "Pro", true],
                [17, "rok_devops", "DevOps", "rok@ops.cloud", "10.1.0.7", "2024-09-01", "2026-04-21", "Online", 1300, "Infra", "Enterprise", true],
                [18, "leon_ai", "ML Engineer", "leon@ai.net", "10.2.2.2", "2024-09-10", "2026-04-20", "Offline", 740, "AI", "Pro", true],
                [19, "mija_support", "Support", "mija@support.io", "192.168.3.12", "2024-09-15", "2026-04-22", "Online", 250, "Support", "Free", false],
                [20, "urban_sys", "SysAdmin", "urban@sys.local", "10.0.0.99", "2024-09-20", "2026-04-21", "Online", 2000, "Infra", "Enterprise", true]
            ] 
        };
    }

    return { 
        columns: ["INFO", "STATUS", "VERSION"], 
        rows: [["Query executed successfully", "OK", "RustgreSQL 0.1-alpha"]] 
    };
}

const server = http.createServer((req, res) => {
    if (req.method === "GET") {
        res.writeHead(200, {"Content-Type": "text/html"});
        return res.end(html);
    }
    if (req.method === "POST" && req.url === "/query") {
        let body = "";
        req.on("data", chunk => body += chunk);
        req.on("end", () => {
            try {
                let { query } = JSON.parse(body || "{}");
                res.writeHead(200, {"Content-Type": "application/json"});
                res.end(JSON.stringify(fakeDB(query)));
            } catch (e) {
                res.end(JSON.stringify({columns: ["Error"], rows: [["Invalid JSON request"]]}));
            }
        });
        return;
    }
});

server.listen(PORT, () => {
    console.log("\x1b[35m%s\x1b[0m", "🚀 RustgreSQL Pro Editor is live!");
    console.log("\x1b[32m%s\x1b[0m", "   Day Mode: http://localhost:" + PORT);
    console.log("\x1b[34m%s\x1b[0m", "   Try 'SELECT BIG' for 50x50 table test.");
});