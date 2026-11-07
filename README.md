# Khaled Apex

Khaled Apex مستودع مستقل للبحث الكمي وPaper وShadow على Binance USDT-M. استورد من `Khaled` عند `5e2880be7ab2d91415bc7e827ffa866195f5129a`. يحتوي فرع `champion-baseline` على شجرة المصدر نفسها للمقارنة. [سجل الترحيل](docs/MIGRATION.md) · [التدقيق](docs/CODE_AUDIT.md) · [نتائج التحقق](reports/APEX_VALIDATION.md)

| المسار | التشغيل | الحالة |
| --- | --- | --- |
| Paper | `python run_bot.py` | Cascade الموروث مع سقف فعلي للمخاطرة وحساب افتراضي |
| Research | `python -m research.replay --help` | محاكاة مستقلة ببيانات أحداث مدخلة، وتوجد دراسة OHLCV منفصلة |
| Shadow | `python -m market_data.capture --help` | بيانات عامة وقرارات افتراضية، تهيئة الاتصال الحي قيد التحقق |
| Demo | `run_demo.py` | Cascade الموروث والتنفيذ الدائم؛ التحقق محلي عبر المحاكي والتشغيل الفعلي معلق |
| Mainnet | — | معطل في طبقة التنفيذ |

## البدء من Termux

داخل Ubuntu في Termux، مع Python 3.11 أو أحدث:

```bash
apt install -y git python3 python3-venv
git clone --branch main https://github.com/khaledhasneen1993/Khaled-Apex.git
cd Khaled-Apex
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python run_bot.py
```

أوقف Paper بـ **Ctrl+C** وانتظر حفظ الحالة. الحالة الافتراضية في `runtime/paper/` ويبدأ الحساب الجديد بـ100 USDT افتراضية. مبلغ 100 USDT في إعداد Paper سقف اسمي. يعتمد الحجم الفعلي على حقوق الحساب والوقف والرسوم والانزلاق المفترض ودقة العقد. قد ترفض الأداة الصفقة إذا كان الحجم الآمن أصغر من الحد الأدنى للمنصة.

لتشغيل لوحة Paper الاختيارية ومقارنة Final في عملية وحساب منفصلين، استخدم `bash run_paper_dashboard.sh --no-autostart` ثم افتح [اللوحة المحلية](http://127.0.0.1:8787) واختر الحساب المطلوب. للتصدير اختر سجل الصفقات ثم CSV أو حمّل لقطة JSON. راجع [دليل Paper](docs/PAPER_RUNBOOK.md) و[دليل Demo](docs/DEMO_RUNBOOK.md).

## الاختبارات

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
python tools/audit_secrets.py
```

تمنع اختبارات التداول الاتصالات الحقيقية وتستخدم عملاء بيانات وهميين ومحاكي تنفيذ. تشغيل الأبحاث يقرأ ملفات السوق. مسار Demo محمي بتفعيل صريح وعناوين اتصال محددة.

## المكونات

- [البنية](docs/ARCHITECTURE.md) و[الاستراتيجيات](docs/STRATEGIES.md)
- [النماذج](docs/AI_MODELS.md) و[المخاطر](docs/RISK_MANAGEMENT.md)
- [التنفيذ](docs/EXECUTION.md) و[الأبحاث](docs/BACKTESTING.md)
- [الأمان](docs/SECURITY.md) و[الحدود الحالية](docs/KNOWN_LIMITATIONS.md)
- [سجل التغييرات](CHANGELOG.md)
