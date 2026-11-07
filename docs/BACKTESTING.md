# إعادة التشغيل والتحقق

المسار الأول Replay لملف JSONL مسجل باستخدام واجهات Binance العامة. لكل حدث `received_ms` و`symbol` و`kind` و`data`. يلزم ضبط ساعة الجهاز. مرشحات الكمية والحد الأدنى يجب أن تخص الفترة المدروسة.

```bash
mkdir -p runtime/research
python -m tools.apex_filters runtime/research/filters.json BTCUSDT ETHUSDT
python -m market_data.capture runtime/research/events.jsonl BTCUSDT ETHUSDT --seconds 900
python -m research.replay runtime/research/events.jsonl runtime/research/filters.json runtime/research/replay-1
python -m research.stress runtime/research/events.jsonl runtime/research/filters.json runtime/research/stress-1.json
python -m tools.apex_dashboard runtime/research/replay-1
```

للتقييم مباشرة أثناء الالتقاط، أضف `--shadow-output runtime/research/shadow-1 --instruments runtime/research/filters.json` إلى أمر capture. اللوحة تعرض تقريرًا محفوظًا، وتفتح محليًا على [127.0.0.1:8788](http://127.0.0.1:8788).

يسعّر Replay بالعمق الظاهر ويحفظ رسومًا وانزلاقًا وزمن انتظار. يمكن إدراج `funding_settlement` موثقًا عند زمن التسوية مع rate وmark؛ تقدير البث ليس تسوية تاريخية، لذلك لا يولّد الملتقط تسويات افتراضية. يحتفظ التقرير بعدد تسويات التمويل وحالة البيانات والأوامر الافتراضية المفتوحة. تغطية التمويل شرط قبل استخدام نتائج Replay للمقارنة.

المسار الثاني `python -m research.ohlcv ... --from-date 2026-10-22 --output study.json`. يحسب دراسة stop sensitivity على مرشح Cascade بالسعر والحجم، ويدخل عند افتتاح الشمعة التالية بتكاليف معلنة. يقسّم عند فجوات الشموع، ويحتسب حد الوقف الساري قبل أي تحديث trailing داخل الشمعة نفسها. لا يعيد بناء دفتر أوامر أو ترتيب أحداث داخل الشمعة. يعرض [تقرير الاختبار](../reports/APEX_VALIDATION.md) نطاق الدليل.

ينتج Replay Profit Factor وdrawdown وexpectancy ونسب الربح والخسارة والتعرّض وتكرار التداول وفئات Regime/Strategy، ويخرج block-bootstrap عند توفر حد أدنى من الصفقات. Sharpe وSortino يتطلبان سلسلة عائد منتظمة. عند عدم توفرها يترك تقرير Replay القيم فارغة. يشترط تقييم الترقية نتائج OOS بعد التكاليف ومقارنةً زوجية كافية واستعادةً مثبتة وموافقة مشغّل.

يقرأ `python -m evaluation.paired runtime/research/events.jsonl` التسجيل نفسه لحساب نوافذ توافق إشارات Champion Proxy وChallenger عندما تكتمل البيانات لكليهما. هذه مقارنة إشارات أولية. قياس زمن محرك القرار متاح عبر `python -m tools.benchmark_apex EVENTS FILTERS`.

## تجارب التعبئة والخروج

```bash
python -m research.replay runtime/research/events.jsonl runtime/research/filters.json \
  runtime/research/experiment-1 --partial-fills --scale-out-fraction 0.5 --scale-out-r 1
```

`--partial-fills` يسمح بتعبئة IOC جزئية حسب العمق المتاح والحد الأدنى عند الدخول. عند تفعيل وقف السوق يبقى إغلاق الباقي معلقًا إلى أن يظهر عمق جديد. لا يُعاد احتساب الكمية الظاهرة المستهلكة وهي ثابتة. تعطل الدفتر يمنع تعبئة جديدة. لا يحاكي النموذج ترتيب الطابور أو السيولة الخفية. تجربة Scale-out الاختيارية تقيد السعر بعتبة الربح المحددة. ويمكن اختبار `--thesis-timeout-ms` عند زوال نظام السوق الذي استندت إليه الإشارة. جميع هذه التجارب معزولة عن التنفيذ الحقيقي.

يسجل `exit_fills.csv` الكمية والسعر والرسوم والتمويل وصافي كل تعبئة. صف `trades.csv` يمثل رحلة مكتملة بعد إغلاق الباقي. يحتوي التقرير أيضًا صافي التعبئات المغلقة للمراكز المفتوحة جزئيًا.

## مقارنة بتكاليف موحدة

```bash
python -m evaluation.costed runtime/research/events.jsonl runtime/research/filters.json \
  runtime/research/costed-1.json --initial-equity 100
```

تشغّل الأداة مرشحًا يقارب إشارة Champion ووقفه وملاحقته أمام Apex، باستخدام التسجيل نفسه والتأخير والميزانية والعمق والرسوم وأحداث التمويل نفسها. الوقف الأولي للمرشح محسوب من السعر المرجعي لحظة القرار. تُستخدم ميزانية مخاطرة مشتركة بدل تحجيم Champion التاريخي الثابت. لذلك توضح المقارنة فرق السياسات داخل نموذج بحثي مشترك. نسخة Champion المرجعية الفعلية موجودة في فرعها. التقرير التجريبي لا يمنح أهلية الترقية.
