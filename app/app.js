const http = require("http");

const PORT = 3000;
// Pot do tvojega delujočega Rust strežnika
const RUST_BACKEND_URL = "http://127.0.0.1:8080";

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
            overflow: hidden;
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

        /* Result Area */
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
            overflow: auto;
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
            white-space: nowrap;
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
    let tabs = [
         {
             query: \`SELECT * FROM starwars 
WHERE height != 172 
AND hair_color = 'brown' 
AND species = 'Human' 
AND height/2 + 100 > mass 
ORDER BY height DESC;\`, 
             result: null
         },
        {
             query: "SELECT oid, relname FROM rg_class LIMIT 5;", 
             result: null
         },
         {
             query: "CREATE TABLE users (id INT, ime VARCHAR, aktiven BOOLEAN);", 
             result: null
         },
         {
             query: "INSERT INTO users VALUES (1, 'Luka', true), (2, 'Valentin', false);", 
             result: null},
        {
             query: "UPDATE users SET aktiven = false WHERE id = 1;", 
             result: null
         }
     ];
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
            
            if (data.status === "error") {
                document.getElementById("result").innerHTML = \`<div style="padding:40px; color:red;">Baza javlja napako: \${data.message}</div>\`;
                return;
            }

            let tableHtml = renderTable(data);
            tabs[currentTabIdx].result = tableHtml;
            document.getElementById("result").innerHTML = tableHtml;
            
            addToHistory(query);
        } catch (e) {
            document.getElementById("result").innerHTML = '<div style="padding:40px; color:red;">Network Error: Backend not reachable</div>';
        }
    }

    function renderTable(data) {
        // POPRAVEK: Sinhronizacija s tvojim Rust WebTranslatorjem, ki podatke vrne v data.data in stolpce v data.columns
        const columns = data.columns;
        const rows = data.data; 

        if (!columns || columns.length === 0 || !rows || rows.length === 0) {
            return '<div style="padding:20px; color:var(--text-dim);">No data returned or empty table.</div>';
        }
        
        let html = "<table><thead><tr>" + columns.map(c => \`<th>\${c}</th>\`).join("") + "</tr></thead><tbody>";
        
        rows.forEach(rowObject => {
            html += "<tr>";
            // Mapiramo objekt v celice glede na vrstni red stolpcev v shemi
            columns.forEach(colName => {
                let cellValue = rowObject[colName];
                if (cellValue === null || cellValue === undefined) cellValue = "NULL";
                html += \`<td>\${cellValue}</td>\`;
            });
            html += "</tr>";
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
                let parsed = JSON.parse(body || "{}");
                let sqlQuery = parsed.query.trim(); // Očistimo morebitne odvečne presledke

                // Pretvorimo string v Buffer, da natančno izmerimo dolžino v bajtih
                const payload = Buffer.from(sqlQuery, "utf-8");

                // --- POPRAVLJEN BACKEND PROXY ---
                const rustRequest = http.request(RUST_BACKEND_URL, {
                    method: "POST",
                    headers: { 
                        "Content-Type": "text/plain; charset=utf-8",
                        // KLJUČNI POPRAVEK: Izklopimo chunked prenos in eksplicitno povemo dolžino
                        "Content-Length": payload.length 
                    }
                }, (rustRes) => {
                    let rustData = "";
                    rustRes.on("data", chunk => rustData += chunk);
                    rustRes.on("end", () => {
                        res.writeHead(rustRes.statusCode, {"Content-Type": "application/json"});
                        res.end(rustData);
                    });
                });

                rustRequest.on("error", (err) => {
                    res.writeHead(502, {"Content-Type": "application/json"});
                    res.end(JSON.stringify({
                        status: "error",
                        message: "Rustgress backend na vratih 8080 ni dosegljiv."
                    }));
                });

                // Pošljemo pripravljen payload
                rustRequest.write(payload);
                rustRequest.end();

            } catch (e) {
                res.writeHead(400, {"Content-Type": "application/json"});
                res.end(JSON.stringify({status: "error", message: "Napačna JSON zahteva"}));
            }
        });
        return;
    }
});

server.listen(PORT, () => {
    console.log("\x1b[35m%s\x1b[0m", "🚀 RustgreSQL Pro Editor je povezan z Rustgress bazo!");
    console.log("\x1b[32m%s\x1b[0m", "   Odpri v brskalniku: http://localhost:" + PORT);
});