# دليل تشغيل Paper

1. ثبّت Python 3.11+ و`requirements.txt` حسب README.
2. نفّذ `python run_bot.py`. تبدأ الحالة الجديدة تحت `runtime/paper/` برصيد 100 USDT افتراضي.
3. اضغط Ctrl+C وانتظر الإغلاق. التشغيل مرة ثانية يستأنف الدفتر نفسه.
4. للتصدير عبر اللوحة، شغّل `bash run_paper_dashboard.sh --no-autostart`، وافتح [اللوحة](http://127.0.0.1:8787)، وشغّل Paper المطلوب. توفر اللوحة CSV للتنفيذات والصفقات المغلقة وJSON للقطة. جلسة اللوحة منفصلة عن `run_bot.py`، وتخزن جلساتها تحت `runtime/paper-dashboard/`.
5. الخيار `--fresh` للوحة ينشئ جلسة جديدة ويحتفظ بالجلسات السابقة. أوقف عمليات الجلسة الحالية قبل الاستئناف في عملية ثانية.

Apex Shadow حساب أبحاث مستقل. راجع [دليل Replay](BACKTESTING.md) لتسجيل الإشارات وتصديرها.

لتصدير دفتر `run_bot.py` بعد الإيقاف:

```bash
python -m tools.paper_export runtime/paper/state.json runtime/paper/export-1
```

## تشغيل Apex Paper

داخل مجلد المشروع على Ubuntu في Termux:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
mkdir -p runtime/apex-paper
.venv/bin/python -m tools.apex_filters runtime/apex-paper/filters.json BTCUSDT ETHUSDT
.venv/bin/python run_bot.py --strategy apex --new-session --instruments runtime/apex-paper/filters.json
```

الأمر الافتراضي `run_bot.py` يستخدم Legacy Paper. لتحديده صراحة استخدم `--strategy legacy`. يبدأ Apex Paper بـ100 USDT افتراضية ويمكن تحديد `--equity` و`--thesis-timeout-ms` عند إنشاء جلسة جديدة فقط. يشغّل Apex الاستراتيجيات المفعلة في `apex.json` بقواعد السوق وإدارة المخاطرة. يرفض الحد الأدنى للكمية أو القيمة عندما لا تسمح الميزانية.

أوقف العملية بـCtrl+C. للاستئناف استخدم `python run_bot.py --strategy apex`. أبقِ `--apex-session` نفسه عند التشغيل والاستئناف إذا اخترت مسارًا غير الافتراضي. يُعاد بناء المحفظة والمراكز والوقف والحساب من السجل، وتلغى نوايا الدخول الافتراضية القديمة على حدود الجلسة. يرفض قفل محلي كاتبًا ثانيًا.

```bash
.venv/bin/python run_bot.py --strategy apex --export runtime/apex-paper/export-1
.venv/bin/python -m tools.apex_dashboard runtime/apex-paper/default
```

افتح `http://127.0.0.1:8788` لمتابعة الحالة. يحتوي مجلد الجلسة على `status.json` و`events.jsonl`. التصدير يتطلب إيقاف كاتب الجلسة، ويحفظ الصفقات والأوامر وتعبئات الخروج في CSV وJSON. دفتر الجلسة قابل لإعادة التشغيل من بدايته، لذلك تزداد مساحة التخزين وزمن الاستئناف مع طول الجلسة. لا تعتمد الحماية المحاكاة على لوحة التحكم. راجع [التسجيل وShadow](BACKTESTING.md).

للتحديث استخدم `git pull --ff-only` من الفرع المثبّت بعد إيقاف العملية. الجلسات و`.env` مستثناة من Git. لمقارنة إصدار على جلسة قائمة، حافظ على إعدادات الجلسة الأصلية وأعد الاختبار على نسخة منفصلة عند تغيّر صيغة الدفتر أو سياسة المحاكاة.
