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
