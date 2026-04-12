# 🚀 Viral Reel Finder — Deploy in 10 Minutes (Railway)

Railway is free, no credit card needed, and gives you a public URL
your manager can open from any device anywhere in the world.

---

## Step 1 — Upload your Facebook cookies to Railway

Your `fb_cookies.txt` file needs to be on the server.
The easiest way: set it as an environment variable.

**Convert your cookies file to one line:**

Open PowerShell and run:
```powershell
$content = Get-Content fb_cookies.txt -Raw
$content | Set-Clipboard
```
This copies the entire file content to your clipboard.

---

## Step 2 — Create Railway account and deploy

1. Go to **https://railway.app** and sign up (free, use GitHub login)

2. Click **"New Project"** → **"Deploy from GitHub repo"**

3. Push this folder to a GitHub repo first:
   ```
   git init
   git add .
   git commit -m "viral reel finder"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/viral-reel-finder.git
   git push -u origin main
   ```

4. In Railway, select your repo → it auto-detects and deploys

---

## Step 3 — Add environment variables in Railway

In Railway dashboard → your project → **Variables** tab, add:

| Variable | Value |
|---|---|
| `FB_COOKIES_FILE` | `/app/fb_cookies.txt` |
| `PORT` | `5000` |

Then go to **Settings** → **Variables** → add a new file variable:
- Click **"New Variable"** → type `FB_COOKIES_CONTENT`
- Paste the full contents of your fb_cookies.txt

Add this to app.py automatically handles it — see note below.

---

## Easier cookie approach — environment variable

Instead of a file, store cookies as an env var. In Railway Variables:

```
FB_COOKIES_CONTENT = (paste entire fb_cookies.txt content here)
```

The app will auto-detect and write it to disk at startup.

---

## Step 4 — Get your public URL

Railway gives you a URL like:
```
https://viral-reel-finder-production.up.railway.app
```

Send this URL to your manager. That's it.
He opens it, types a keyword, clicks Search. Done.

---

## Step 5 — Re-export cookies when they expire

Facebook cookies expire every ~60 days.
When search stops working, just re-export from browser,
update the Railway environment variable, and redeploy.

---

## Free tier limits

Railway free tier gives you:
- $5/month in credits (enough for ~500 hours)
- 1GB RAM
- Sleeps after 30 min of inactivity (wakes up on first request, ~10s delay)

For always-on, upgrade to Hobby plan ($5/month).

---

## Local testing before deploying

```powershell
pip install flask gunicorn
python app.py
```
Open http://localhost:5000 in browser.
