# Khaled Apex

Khaled Apex منظومة بحث وتنفيذ افتراضي لعقود Binance USDⓈ-M Futures. تجمع Cascade Continuation وLiquidity Reversal وRange Mean Reversion، وتستخدم نظام السوق والتكلفة والسيولة لترتيب الفرص. تعيد طبقة المخاطرة فحص الحجم والهامش والارتباط والوقف قبل التنفيذ. يحفظ Paper الأحداث، ويحفظ Demo النوايا والتعبئات والحماية الأصلية والمصالحة. يحفظ [Champion Baseline](https://github.com/khaledhasneen1993/Khaled-Apex/tree/champion-baseline) مرجع المقارنة.

| المسار | الحالة |
| --- | --- |
| Legacy Paper | المسار الافتراضي في `run_bot.py` |
| Apex Paper | القواعد الثلاث وجلسة افتراضية قابلة للاستئناف |
| Shadow وReplay | تسجيل أحداث السوق وإعادة تشغيلها دون اتصال مالي |
| Apex Demo Preflight | أداة قراءة فقط |
| Apex Demo Qualification | مسار مستقل مقيد، يتطلب موافقة المشغّل وفحص الحساب وقفلًا مشتركًا |
| Apex Demo Operational Release | معلق إلى حين تحقق الأدلة التشغيلية ومراجعتها |
| Mainnet | معطل |

## التثبيت على Ubuntu داخل Termux

```bash
apt install -y git python3 python3-venv
git clone https://github.com/khaledhasneen1993/Khaled-Apex.git
cd Khaled-Apex
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
```

### Apex Paper

```bash
mkdir -p runtime/apex-paper
python -m tools.apex_filters runtime/apex-paper/filters.json BTCUSDT ETHUSDT
python run_bot.py --strategy apex --new-session --instruments runtime/apex-paper/filters.json
```

الرصيد الافتراضي 100 USDT، و`--equity` يحدد رصيد جلسة جديدة. ضبط الاستراتيجيات والحدود في `apex.json`. أوقف العملية بـCtrl+C، واستأنفها بـ`python run_bot.py --strategy apex`. يحفظ مسار الجلسة الافتراضي `runtime/apex-paper/default/` دفتر الأحداث والحالة. لتشغيل Legacy استخدم `python run_bot.py --strategy legacy`.

```bash
python -m tools.apex_dashboard runtime/apex-paper/default
# بعد إيقاف جلسة Paper
python run_bot.py --strategy apex --export runtime/apex-paper/export-1
```

الحالة على `127.0.0.1:8788`، والتصدير CSV وJSON. لمعرفة تشغيل Shadow والسجلات والاستئناف بمسار مخصص راجع [Paper](docs/PAPER_RUNBOOK.md) و[Replay وShadow](docs/BACKTESTING.md).

### Demo

افصل حساب Demo ومفاتيحه عن أي عملية أخرى. يتطلب التأهيل حسابًا مستقلًا خاليًا من المراكز والأوامر، وإعداد One-way وSingle-Asset وIsolated لكل رمز، وموافقة خاصة ببصمة الإصدار والإعدادات، وقفلًا مركزيًا مشتركًا. سقف التأهيل 10×، ومخاطرة مقدّرة 0.25% لكل دخول، ومركز واحد، وحد خسارة يومي 0.5% يحجب الدخول. راجع [الفحص والموافقة والتأهيل](docs/DEMO_RUNBOOK.md). خطوات التثبيت أعلاه لا تبدأ Demo.

## الذكاء الاصطناعي والأدلة

مسارات Paper وDemo تعمل بالقواعد. التدريب منفصل ويستخدم عينات point-in-time وحواجز الهدف والوقف والمهلة وفصلًا زمنيًا مع embargo ومعايرةً. يحتفظ الأثر ببصمة البيانات والنوافذ. درجات القواعد والنموذج البحثي منفصلة عن أفضلية صافية مثبتة. المقارنة الحالية تستخدم مرجع إشارة Cascade مع افتراضات تنفيذ موحدة. لا تتوفر نتائج سوق كافية لاعتماد AI أو إثبات تفوق استراتيجية.

## الاختبارات والوثائق

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m tools.audit_secrets
```

اختبارات التداول معزولة عن الشبكة المالية. GitHub Actions يشغل Python 3.11 و3.12. راجع [حالة الإصدار](reports/APEX_VALIDATION.md)، [البنية](docs/ARCHITECTURE.md)، [الاستراتيجيات](docs/STRATEGIES.md)، [المخاطر](docs/RISK_MANAGEMENT.md)، [التنفيذ](docs/EXECUTION.md)، [النماذج](docs/AI_MODELS.md)، [الأمان](docs/SECURITY.md)، [الحدود](docs/KNOWN_LIMITATIONS.md)، و[التغييرات](CHANGELOG.md).
