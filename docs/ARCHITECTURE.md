# البنية

```mermaid
flowchart TD
  A["Binance public data"] --> B["Capture and sequence checks"]
  B --> C["Point-in-time features"]
  C --> D["Regime and strategies"]
  D --> E["Ranking and optional model"]
  E --> F["Portfolio and sizing"]
  F --> G["Shadow / Replay"]
  G --> H["Report and dashboard"]
```

- `market_data/` يطبّع trades وkline وmark/index/funding وopen interest وأحداث التصفية المتاحة. يستخدم عمقًا متسلسلًا للـ BBO وOFI وmarket impact. بيانات التصفية لقطات متاحة من البث، وتناقص الكمية في الدفتر لا يعرّف الإلغاء وحده.
- `regime/` و`strategies/` و`apex/engine.py` تُخرج قرارات بلا عميل تداول.
- `models/` يفصل التدريب من Python العادي عن الاستدلال المباشر على JSON مع موافقة ملفية تربط بصمة الإعدادات بالنموذج.
- `portfolio/` و`risk/apex_*.py` يحددان حدود تعرض ومحفظة ووقف في المحاكاة.
- `research/` يطبق Replay وOHLCV وتحليل الحساسية. `evaluation/` ينتج أسباب رفض الترقية ولا يبدل أدوار الاستراتيجيات.
- التشغيل الحالي `run_bot.py` و`run_demo.py` يحافظ على scanner و`DynamicPaperHunter` والتنفيذ الدائم. `baseline.json` يحكم التشغيل الموروث؛ `apex.json` يحكم البحث ويملك بصمة مستقلة.
- يحتفظ الالتقاط بطابور محدود ويخرج الكتابة الدورية عن Event Loop. التدريب عملية CLI منفصلة. إعادة التشغيل الكاملة للبحث تقرأ التسجيل من البداية لإعادة بناء الحالة.

## طبقة التشغيل

`apex/runtime.py` يشغل Apex Paper على بث الأحداث العام وسجل أحادي الكاتب يُزامَن إلى القرص قبل تطبيق دفعة الأحداث. يستخدم Replay وPaper نفس مدير النوايا `apex/order_manager.py`. يحتفظ الدخول بحجز الهامش والخسارة إلى أن يصبح الأمر منتهيًا. يقرأ Demo طبقة التطبيع نفسها عبر `apex/demo_runtime.py`، ويمرر القرار إلى `ApexDemoExchange` الذي يرث سجل التنفيذ والمصالحة والحماية. يبدأ مسار Demo خلف بوابة مغلقة في هذا الإصدار.

يتحقق `apex/qualification.py` من موافقة الإصدار والحساب وملكية القفل المشترك. يربط `apex/order_authorization.py` قرار المخاطرة بكمية وسعر ووقف ورافعة وإصدار حالة حساب محددين. يحفظ WAL الخطة والبصمة؛ وتعيد طبقة الإرسال التحقق من الحساب والدفتر وملكية القفل.
