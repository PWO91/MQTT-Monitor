#include <cstdlib>
#include <deque>
#include <map>
#include <memory>

#include <QApplication>
#include <QDateTime>
#include <QDir>
#include <QFile>
#include <QGroupBox>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QLabel>
#include <QLineEdit>
#include <QMainWindow>
#include <QCheckBox>
#include <QClipboard>
#include <QComboBox>
#include <QFileDialog>
#include <QGuiApplication>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QListWidget>
#include <QMenuBar>
#include <QMessageBox>
#include <QSettings>
#include <QTabWidget>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QSpinBox>
#include <QSplitter>
#include <QFormLayout>
#include <QStatusBar>
#include <QStyle>
#include <QTimer>
#include <QStringList>
#include <QTreeWidget>
#include <QTreeWidgetItem>
#include <QVBoxLayout>
#include <QWidget>

#include <mqtt/async_client.h>
#include <mqtt/ssl_options.h>

static QString envOr(const char* name, const char* fallback) {
    const char* val = std::getenv(name);
    return val ? QString(val) : QString(fallback);
}

struct MsgEntry {
    QString timestamp;
    QString payload;
    bool    retained = false;
};

static constexpr int kMaxHistory = 50;

class TopicTree : public mqtt::callback {
    QTreeWidget* widget_;
    QTreeWidgetItem* root_ = nullptr;
    std::map<QString, QTreeWidgetItem*> nodes_;
    std::map<QString, std::deque<MsgEntry>> history_;
    QString brokerAddr_;
    int rxCount_ = 0;

public:
    explicit TopicTree(QTreeWidget* w) : widget_(w) {}

    void reset(const QString& brokerAddr) {
        widget_->clear();
        nodes_.clear();
        history_.clear();
        brokerAddr_ = brokerAddr;
        rxCount_ = 0;
        root_ = new QTreeWidgetItem(widget_);
        root_->setText(0, brokerAddr);
        root_->setExpanded(false);
    }

    int  rxCount()     const { return rxCount_; }
    int  topicCount()  const { return (int)history_.size(); }
    int  nodeCount()   const { return (int)nodes_.size(); }

    // Returns latest payload for topic, or empty string
    QString payloadFor(const QString& topic) const {
        auto it = history_.find(topic);
        if (it == history_.end() || it->second.empty()) return {};
        return it->second.back().payload;
    }

    const std::deque<MsgEntry>* historyFor(const QString& topic) const {
        auto it = history_.find(topic);
        return it != history_.end() ? &it->second : nullptr;
    }

    // Collect all messages as flat list for export: {topic, timestamp, payload, retained}
    QVector<std::tuple<QString,QString,QString,bool>> allMessages() const {
        QVector<std::tuple<QString,QString,QString,bool>> out;
        for (auto& [topic, entries] : history_)
            for (auto& e : entries)
                out.append({topic, e.timestamp, e.payload, e.retained});
        return out;
    }

    void message_arrived(mqtt::const_message_ptr msg) override {
        QString topic    = QString::fromStdString(msg->get_topic());
        QString payload  = QString::fromStdString(msg->to_string());
        bool    retained = msg->is_retained();
        QString ts       = QDateTime::currentDateTime().toString("hh:mm:ss.zzz");

        ++rxCount_;
        QMetaObject::invokeMethod(widget_, [this, topic, payload, retained, ts]() {
            insert(topic, payload, retained, ts);
        }, Qt::QueuedConnection);
    }

private:
    void refreshLabel(QTreeWidgetItem* item, const QString& seg) {
        int n = item->childCount();
        item->setText(0, n > 0 ? QString("%1 (%2)").arg(seg).arg(n) : seg);
    }

    void insert(const QString& topic, const QString& payload,
                bool retained, const QString& ts)
    {
        if (!root_) return;

        auto& hist = history_[topic];
        hist.push_back({ts, payload, retained});
        if ((int)hist.size() > kMaxHistory)
            hist.pop_front();

        QStringList parts = topic.split('/');
        QTreeWidgetItem* parent = root_;
        QString path;
        bool anyNew = false;

        for (const QString& seg : parts) {
            path += (path.isEmpty() ? "" : "/") + seg;
            auto it = nodes_.find(path);
            if (it == nodes_.end()) {
                auto* item = new QTreeWidgetItem(parent);
                item->setText(0, seg);
                item->setExpanded(false);
                nodes_[path] = item;
                anyNew = true;
                parent = item;
            } else {
                parent = it->second;
            }
        }

        if (anyNew) {
            QString p;
            for (const QString& seg : parts) {
                p += (p.isEmpty() ? "" : "/") + seg;
                refreshLabel(nodes_[p], seg);
            }
            int n = root_->childCount();
            root_->setText(0, n > 0
                ? QString("%1 (%2)").arg(brokerAddr_).arg(n)
                : brokerAddr_);
        }

        if (parent != root_) {
            // Column 1: value preview with [R] marker for retained messages
            QString preview = retained
                ? QString("[R] %1").arg(payload)
                : payload;
            if (preview.length() > 100) preview = preview.left(100) + "...";
            parent->setText(1, preview);
            if (retained)
                parent->setForeground(1, QColor(180, 100, 0));
            else
                parent->setForeground(1, QApplication::palette().text().color());

            // Column 2: timestamp
            parent->setText(2, ts);
        }
    }
};

static QString stripCount(const QString& s) {
    if (s.endsWith(')')) {
        int i = s.lastIndexOf(" (");
        if (i != -1) return s.left(i);
    }
    return s;
}

static QString itemTopic(QTreeWidgetItem* item) {
    QStringList parts;
    for (QTreeWidgetItem* cur = item; cur && cur->parent(); cur = cur->parent())
        parts.prepend(stripCount(cur->text(0)));
    return parts.join('/');
}

static QString formatHex(const QString& raw) {
    QByteArray ba = raw.toUtf8();
    QString out;
    for (int i = 0; i < ba.size(); ++i) {
        if (i > 0 && i % 16 == 0) out += '\n';
        out += QString("%1 ").arg((quint8)ba[i], 2, 16, QChar('0')).toUpper();
    }
    return out.trimmed();
}

// mode: 0=topic, 1=value, 2=both
static bool applyFilter(QTreeWidgetItem* item, const QString& text,
                        int mode, const TopicTree* cb)
{
    bool anyChildVisible = false;
    for (int i = 0; i < item->childCount(); ++i)
        if (applyFilter(item->child(i), text, mode, cb)) anyChildVisible = true;

    if (item->parent() == nullptr) {  // root
        item->setHidden(false);
        return true;
    }

    if (text.isEmpty()) {
        item->setHidden(false);
        return true;
    }

    bool topicMatch = (mode == 0 || mode == 2)
        && itemTopic(item).contains(text, Qt::CaseInsensitive);
    bool valueMatch = (mode == 1 || mode == 2) && cb
        && cb->payloadFor(itemTopic(item)).contains(text, Qt::CaseInsensitive);

    bool match = anyChildVisible || topicMatch || valueMatch;
    item->setHidden(!match);
    return match;
}

int main(int argc, char* argv[]) {
    QApplication app(argc, argv);
    app.setOrganizationName("mcrTechLab");
    app.setApplicationName("MQTTMonitor");

    QMainWindow window;
    window.setWindowTitle("MQTT Monitor");
    window.resize(900, 650);

    QWidget* central = new QWidget;
    QVBoxLayout* rootLayout = new QVBoxLayout(central);

    std::unique_ptr<TopicTree> cb;
    std::shared_ptr<mqtt::async_client> client;
    int txCount = 0;
    QDateTime connectedAt;

    // ── Top tabs ────────────────────────────────────────────────────────────
    QTabWidget* topTabs = new QTabWidget;
    topTabs->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Fixed);
    topTabs->setMaximumHeight(80);

    // Tab: Połączenie
    QWidget* connTab = new QWidget;
    QHBoxLayout* row = new QHBoxLayout(connTab);
    row->setContentsMargins(4, 4, 4, 4);

    QLineEdit* addrEdit = new QLineEdit(envOr("MQTT_BROKER", "localhost"));
    addrEdit->setPlaceholderText("Host");
    addrEdit->setMinimumWidth(120);

    QSpinBox* portSpin = new QSpinBox;
    portSpin->setRange(1, 65535);
    portSpin->setValue(1883);
    portSpin->setFixedWidth(70);

    QLineEdit* clientIdEdit = new QLineEdit("mqtt_monitor");
    clientIdEdit->setPlaceholderText("Client ID");
    clientIdEdit->setMinimumWidth(90);

    QLineEdit* userEdit = new QLineEdit(envOr("MQTT_USER", ""));
    userEdit->setPlaceholderText("Username");
    userEdit->setMinimumWidth(90);

    QLineEdit* passEdit = new QLineEdit(envOr("MQTT_PASSWORD", ""));
    passEdit->setPlaceholderText("Password");
    passEdit->setEchoMode(QLineEdit::Password);
    passEdit->setMinimumWidth(90);

    QCheckBox* tlsCheck = new QCheckBox("TLS");
    QPushButton* connectBtn = new QPushButton("Connect");

    row->addWidget(new QLabel("Host:"));     row->addWidget(addrEdit);
    row->addWidget(new QLabel("Port:"));     row->addWidget(portSpin);
    row->addWidget(new QLabel("ID:"));       row->addWidget(clientIdEdit);
    row->addWidget(new QLabel("User:"));     row->addWidget(userEdit);
    row->addWidget(new QLabel("Password:")); row->addWidget(passEdit);
    row->addWidget(tlsCheck);
    row->addWidget(connectBtn);
    topTabs->addTab(connTab, "Connection");

    // TLS toggles default port
    QObject::connect(tlsCheck, &QCheckBox::toggled, [&](bool on) {
        if (on && portSpin->value() == 1883) portSpin->setValue(8883);
        if (!on && portSpin->value() == 8883) portSpin->setValue(1883);
    });

    // Tab: Subskrypcje
    QWidget* subTab = new QWidget;
    QVBoxLayout* subLayout = new QVBoxLayout(subTab);
    subLayout->setContentsMargins(4, 4, 4, 4);

    QListWidget* subList = new QListWidget;
    subList->setFixedHeight(42);
    subList->addItem("#");
    subLayout->addWidget(subList);

    QHBoxLayout* subInputRow = new QHBoxLayout;
    QLineEdit* subEdit = new QLineEdit;
    subEdit->setPlaceholderText("Topic, e.g. sensors/# or home/lamp");
    QPushButton* subAddBtn = new QPushButton("Add");
    QPushButton* subRemBtn = new QPushButton("Remove");
    subInputRow->addWidget(subEdit);
    subInputRow->addWidget(subAddBtn);
    subInputRow->addWidget(subRemBtn);
    subLayout->addLayout(subInputRow);
    topTabs->addTab(subTab, "Subscriptions");

    QObject::connect(subAddBtn, &QPushButton::clicked, [&]() {
        QString t = subEdit->text().trimmed();
        if (t.isEmpty()) return;
        for (int i = 0; i < subList->count(); ++i)
            if (subList->item(i)->text() == t) return;
        subList->addItem(t);
        subEdit->clear();
        if (client && client->is_connected()) {
            try { client->subscribe(t.toStdString(), 0)->wait(); } catch (...) {}
            window.statusBar()->showMessage(QString("Subscribed: %1").arg(t));
        }
    });
    QObject::connect(subEdit, &QLineEdit::returnPressed, subAddBtn, &QPushButton::click);
    QObject::connect(subRemBtn, &QPushButton::clicked, [&]() {
        auto* item = subList->currentItem();
        if (!item) return;
        QString t = item->text();
        if (client && client->is_connected()) {
            try { client->unsubscribe(t.toStdString())->wait(); } catch (...) {}
        }
        delete item;
        window.statusBar()->showMessage(QString("Unsubscribed: %1").arg(t));
    });

    rootLayout->addWidget(topTabs);

    // ── Search bar ──────────────────────────────────────────────────────────
    QHBoxLayout* searchRow = new QHBoxLayout;
    QLineEdit* searchEdit = new QLineEdit;
    searchEdit->setPlaceholderText("Search…");
    searchEdit->setClearButtonEnabled(true);
    QComboBox* searchModeCombo = new QComboBox;
    searchModeCombo->addItem("Topic", 0);
    searchModeCombo->addItem("Value", 1);
    searchModeCombo->addItem("Both",  2);
    searchModeCombo->setFixedWidth(80);
    searchRow->addWidget(new QLabel("Filter:"));
    searchRow->addWidget(searchEdit);
    searchRow->addWidget(searchModeCombo);
    rootLayout->addLayout(searchRow);

    // ── Splitter: tree | detail panel ───────────────────────────────────────
    QSplitter* splitter = new QSplitter(Qt::Vertical);

    QTreeWidget* tree = new QTreeWidget;
    tree->setColumnCount(3);
    tree->setHeaderLabels({"Topic", "Last value", "Time"});
    tree->header()->setStretchLastSection(false);
    tree->header()->setSectionResizeMode(0, QHeaderView::Interactive);
    tree->header()->setSectionResizeMode(1, QHeaderView::Stretch);
    tree->header()->setSectionResizeMode(2, QHeaderView::Fixed);
    tree->header()->resizeSection(0, 240);
    tree->header()->resizeSection(2, 100);
    splitter->addWidget(tree);

    QTabWidget* bottomTabs = new QTabWidget;

    // ── View tab ─────────────────────────────────────────────────────────────
    QWidget* viewTab = new QWidget;
    QVBoxLayout* detailLayout = new QVBoxLayout(viewTab);
    detailLayout->setContentsMargins(4, 4, 4, 4);

    QHBoxLayout* detailHeader = new QHBoxLayout;
    QLabel* topicLabel = new QLabel("Click a topic to see details");
    topicLabel->setStyleSheet("font-weight: bold; color: gray;");

    QComboBox* formatCombo = new QComboBox;
    formatCombo->addItem("Raw",    0);
    formatCombo->addItem("JSON",   1);
    formatCombo->addItem("Hex",    2);
    formatCombo->addItem("Base64", 3);
    formatCombo->setFixedWidth(80);

    QPushButton* copyTopicBtn = new QPushButton("Copy topic");
    copyTopicBtn->setFixedWidth(90);
    QPushButton* copyBtn = new QPushButton("Copy");
    copyBtn->setFixedWidth(70);

    detailHeader->addWidget(topicLabel);
    detailHeader->addStretch();
    detailHeader->addWidget(copyTopicBtn);
    detailHeader->addWidget(new QLabel("Format:"));
    detailHeader->addWidget(formatCombo);
    detailHeader->addWidget(copyBtn);
    detailLayout->addLayout(detailHeader);

    QPlainTextEdit* payloadView = new QPlainTextEdit;
    payloadView->setReadOnly(true);
    payloadView->setPlaceholderText("(no data)");
    payloadView->setFont(QFont("Menlo, Monaco, Courier New", 11));
    detailLayout->addWidget(payloadView);

    bottomTabs->addTab(viewTab, "View");

    // ── Historia tab ──────────────────────────────────────────────────────────
    QWidget* histTab = new QWidget;
    QVBoxLayout* histLayout = new QVBoxLayout(histTab);
    histLayout->setContentsMargins(4, 4, 4, 4);

    QListWidget* histList = new QListWidget;
    histList->setFont(QFont("Menlo, Monaco, Courier New", 10));
    histLayout->addWidget(histList);

    QPlainTextEdit* histDetail = new QPlainTextEdit;
    histDetail->setReadOnly(true);
    histDetail->setFont(QFont("Menlo, Monaco, Courier New", 11));
    histDetail->setMaximumHeight(100);
    histLayout->addWidget(histDetail);

    bottomTabs->addTab(histTab, "History");

    // ── Publish tab ───────────────────────────────────────────────────────────
    QWidget* publishTab = new QWidget;
    QVBoxLayout* pubLayout = new QVBoxLayout(publishTab);
    pubLayout->setContentsMargins(4, 4, 4, 4);

    QHBoxLayout* pubTopicRow = new QHBoxLayout;
    pubTopicRow->addWidget(new QLabel("Topic:"));
    QLineEdit* pubTopicEdit = new QLineEdit;
    pubTopicEdit->setPlaceholderText("e.g. gateways/11330999/cmd");
    pubTopicRow->addWidget(pubTopicEdit);
    pubLayout->addLayout(pubTopicRow);

    QPlainTextEdit* pubMessageEdit = new QPlainTextEdit;
    pubMessageEdit->setPlaceholderText("Message payload...");
    pubMessageEdit->setFont(QFont("Menlo, Monaco, Courier New", 11));
    pubLayout->addWidget(pubMessageEdit);

    QHBoxLayout* pubOptionsRow = new QHBoxLayout;
    pubOptionsRow->addWidget(new QLabel("QoS:"));
    QComboBox* qosCombo = new QComboBox;
    qosCombo->addItem("0 — At most once",  0);
    qosCombo->addItem("1 — At least once", 1);
    qosCombo->addItem("2 — Exactly once",  2);
    qosCombo->setFixedWidth(160);
    pubOptionsRow->addWidget(qosCombo);
    QCheckBox* retainedCheck = new QCheckBox("Retained");
    pubOptionsRow->addWidget(retainedCheck);
    pubOptionsRow->addStretch();
    QPushButton* publishBtn = new QPushButton("Publish");
    pubOptionsRow->addWidget(publishBtn);
    pubLayout->addLayout(pubOptionsRow);

    bottomTabs->addTab(publishTab, "Publish");

    // ── Diagnostyka tab ───────────────────────────────────────────────────────
    QWidget* diagTab = new QWidget;
    QVBoxLayout* diagLayout = new QVBoxLayout(diagTab);
    diagLayout->setContentsMargins(12, 8, 12, 8);

    QFormLayout* diagForm = new QFormLayout;
    diagForm->setHorizontalSpacing(20);
    diagForm->setVerticalSpacing(6);

    auto makeStat = [&](const QString& label) -> QLabel* {
        auto* val = new QLabel("—");
        val->setFont(QFont("Menlo, Monaco, Courier New", 12));
        diagForm->addRow(label, val);
        return val;
    };

    QLabel* statRx       = makeStat("Messages received:");
    QLabel* statTx       = makeStat("Messages sent:");
    QLabel* statTopics   = makeStat("Unique topics:");
    QLabel* statNodes    = makeStat("Tree nodes:");
    QLabel* statUptime   = makeStat("Uptime:");
    QLabel* statConnTime = makeStat("Connected since:");

    diagLayout->addLayout(diagForm);
    diagLayout->addStretch();
    bottomTabs->addTab(diagTab, "Diagnostics");

    splitter->addWidget(bottomTabs);
    splitter->setStretchFactor(0, 3);
    splitter->setStretchFactor(1, 1);
    rootLayout->addWidget(splitter);

    window.setCentralWidget(central);
    window.statusBar()->showMessage("Ready");

    // ── Logic ─────────────────────────────────────────────────────────────────
    QString currentTopic;

    // Diagnostics: refresh stats every second
    auto updateDiag = [&]() {
        if (!cb || !client || !client->is_connected()) {
            statRx->setText("—");
            statTx->setText("—");
            statTopics->setText("—");
            statNodes->setText("—");
            statUptime->setText("—");
            statConnTime->setText("—");
            return;
        }
        statRx->setText(QString::number(cb->rxCount()));
        statTx->setText(QString::number(txCount));
        statTopics->setText(QString::number(cb->topicCount()));
        statNodes->setText(QString::number(cb->nodeCount()));
        statConnTime->setText(connectedAt.toString("hh:mm:ss"));
        qint64 secs = connectedAt.secsTo(QDateTime::currentDateTime());
        statUptime->setText(QString("%1:%2:%3")
            .arg(secs / 3600, 2, 10, QChar('0'))
            .arg((secs % 3600) / 60, 2, 10, QChar('0'))
            .arg(secs % 60, 2, 10, QChar('0')));
    };

    QTimer* diagTimer = new QTimer(&window);
    diagTimer->setInterval(1000);
    QObject::connect(diagTimer, &QTimer::timeout, [&]() { updateDiag(); });
    diagTimer->start();

    // Formats raw payload according to selected format
    auto formatPayload = [&](const QString& raw) -> QString {
        int fmt = formatCombo->currentData().toInt();
        if (fmt == 1) {  // JSON
            QJsonParseError err;
            QJsonDocument doc = QJsonDocument::fromJson(raw.toUtf8(), &err);
            if (err.error == QJsonParseError::NoError)
                return doc.toJson(QJsonDocument::Indented);
        }
        if (fmt == 2) return formatHex(raw);  // Hex
        if (fmt == 3) return QString::fromLatin1(raw.toUtf8().toBase64());  // Base64
        return raw;  // Raw
    };

    auto showPayload = [&](const QString& topic, const QString& raw) {
        topicLabel->setText(topic);
        topicLabel->setStyleSheet("font-weight: bold; color: black;");
        if (raw.isEmpty()) {
            payloadView->setPlainText("(node has no messages)");
            return;
        }
        payloadView->setPlainText(formatPayload(raw));
    };

    auto showHistory = [&](const QString& topic) {
        histList->clear();
        histDetail->clear();
        if (!cb) return;
        const auto* hist = cb->historyFor(topic);
        if (!hist) return;
        // Show newest first
        for (int i = (int)hist->size() - 1; i >= 0; --i) {
            const auto& e = (*hist)[i];
            QString preview = e.payload.length() > 80
                ? e.payload.left(80) + "..."
                : e.payload;
            QString label = QString("[%1]%2 %3")
                .arg(e.timestamp)
                .arg(e.retained ? " [R]" : "")
                .arg(preview);
            histList->addItem(label);
            histList->item(histList->count() - 1)->setData(Qt::UserRole, e.payload);
            if (e.retained)
                histList->item(histList->count() - 1)->setForeground(QColor(180, 100, 0));
        }
    };

    QObject::connect(histList, &QListWidget::itemClicked, [&](QListWidgetItem* item) {
        histDetail->setPlainText(item->data(Qt::UserRole).toString());
    });

    QObject::connect(copyTopicBtn, &QPushButton::clicked, [&]() {
        if (currentTopic.isEmpty()) return;
        QGuiApplication::clipboard()->setText(currentTopic);
        window.statusBar()->showMessage(QString("Topic copied: %1").arg(currentTopic));
    });

    QObject::connect(copyBtn, &QPushButton::clicked, [&]() {
        QGuiApplication::clipboard()->setText(payloadView->toPlainText());
        window.statusBar()->showMessage("Message copied to clipboard.");
    });

    QObject::connect(tree, &QTreeWidget::itemClicked, [&](QTreeWidgetItem* item, int) {
        if (!cb) return;
        currentTopic = itemTopic(item);
        if (currentTopic.isEmpty()) return;
        showPayload(currentTopic, cb->payloadFor(currentTopic));
        showHistory(currentTopic);
        pubTopicEdit->setText(currentTopic);
    });

    QObject::connect(formatCombo, &QComboBox::currentIndexChanged, [&]() {
        if (!cb || currentTopic.isEmpty()) return;
        showPayload(currentTopic, cb->payloadFor(currentTopic));
    });

    QObject::connect(publishBtn, &QPushButton::clicked, [&]() {
        if (!client || !client->is_connected()) {
            window.statusBar()->showMessage("Not connected to broker.");
            return;
        }
        QString topic   = pubTopicEdit->text().trimmed();
        QString message = pubMessageEdit->toPlainText();
        if (topic.isEmpty()) {
            window.statusBar()->showMessage("Enter a topic before publishing.");
            return;
        }
        try {
            int  qos      = qosCombo->currentData().toInt();
            bool retained = retainedCheck->isChecked();
            auto msg = mqtt::make_message(topic.toStdString(), message.toStdString(), qos, retained);
            client->publish(msg)->wait();
            ++txCount;
            window.statusBar()->showMessage(
                QString("Published to '%1' (QoS %2%3)")
                    .arg(topic).arg(qos).arg(retained ? ", retained" : ""));
        } catch (const mqtt::exception& e) {
            window.statusBar()->showMessage(QString("Publish error: %1").arg(e.what()));
        }
    });

    // Search/filter
    auto runFilter = [&]() {
        const QString text = searchEdit->text();
        const int mode = searchModeCombo->currentData().toInt();
        for (int i = 0; i < tree->topLevelItemCount(); ++i)
            applyFilter(tree->topLevelItem(i), text, mode, cb.get());
    };
    QObject::connect(searchEdit, &QLineEdit::textChanged, [&](const QString&) { runFilter(); });
    QObject::connect(searchModeCombo, &QComboBox::currentIndexChanged, [&](int) { runFilter(); });

    // Connect / disconnect
    QObject::connect(connectBtn, &QPushButton::clicked, [&]() {
        if (client && client->is_connected()) {
            try { client->disconnect()->wait(); } catch (...) {}
            client.reset();
            cb.reset();
            connectBtn->setText("Connect");
            window.statusBar()->showMessage("Disconnected.");
            return;
        }

        QString addr     = addrEdit->text().trimmed();
        int     port     = portSpin->value();
        QString clientId = clientIdEdit->text().trimmed();
        QString user     = userEdit->text().trimmed();
        QString pass     = passEdit->text();
        bool    useTls   = tlsCheck->isChecked();

        if (addr.isEmpty()) {
            window.statusBar()->showMessage("Enter broker address.");
            return;
        }
        if (clientId.isEmpty()) clientId = "mqtt_monitor";

        connectBtn->setEnabled(false);
        window.statusBar()->showMessage(
            QString("Connecting to %1:%2%3...").arg(addr).arg(port).arg(useTls ? " (TLS)" : ""));

        cb = std::make_unique<TopicTree>(tree);
        cb->reset(addr);

        QString scheme = useTls ? "ssl" : "tcp";
        QString uri    = QString("%1://%2:%3").arg(scheme).arg(addr).arg(port);
        client = std::make_shared<mqtt::async_client>(uri.toStdString(), clientId.toStdString());
        client->set_callback(*cb);

        mqtt::connect_options opts;
        opts.set_keep_alive_interval(20);
        opts.set_clean_session(true);
        if (!user.isEmpty()) {
            opts.set_user_name(user.toStdString());
            opts.set_password(pass.toStdString());
        }
        if (useTls) {
            mqtt::ssl_options ssl;
            ssl.set_verify(false);  // dev-friendly: skip cert verification
            opts.set_ssl(ssl);
        }

        try {
            client->connect(opts)->wait();
            QStringList topics;
            for (int i = 0; i < subList->count(); ++i)
                topics << subList->item(i)->text();
            for (const QString& t : topics)
                client->subscribe(t.toStdString(), 0)->wait();
            txCount = 0;
            connectedAt = QDateTime::currentDateTime();
            connectBtn->setText("Disconnect");
            window.statusBar()->showMessage(
                QString("Connected to %1:%2 — %3 subscription(s)").arg(addr).arg(port).arg(topics.size()));
        } catch (const mqtt::exception& e) {
            window.statusBar()->showMessage(QString("Error: %1").arg(e.what()));
            client.reset();
            cb.reset();
        }
        connectBtn->setEnabled(true);
    });

    // ── Settings persistence ──────────────────────────────────────────────────
    auto collectJson = [&]() -> QJsonObject {
        QJsonArray subs;
        for (int i = 0; i < subList->count(); ++i)
            subs.append(subList->item(i)->text());
        return QJsonObject{
            {"broker",        addrEdit->text()},
            {"port",          portSpin->value()},
            {"clientId",      clientIdEdit->text()},
            {"user",          userEdit->text()},
            {"password",      passEdit->text()},
            {"tls",           tlsCheck->isChecked()},
            {"subscriptions", subs}
        };
    };

    auto applyJson = [&](const QJsonObject& obj) {
        addrEdit->setText(obj["broker"].toString());
        portSpin->setValue(obj["port"].toInt(1883));
        clientIdEdit->setText(obj.value("clientId").toString("mqtt_monitor"));
        userEdit->setText(obj["user"].toString());
        passEdit->setText(obj["password"].toString());
        tlsCheck->setChecked(obj.value("tls").toBool(false));
        subList->clear();
        for (const auto& v : obj["subscriptions"].toArray())
            subList->addItem(v.toString());
        if (subList->count() == 0)
            subList->addItem("#");
    };

    // Auto-restore last session
    {
        QSettings s;
        if (s.contains("broker")) {
            QJsonObject obj;
            obj["broker"]   = s.value("broker").toString();
            obj["port"]     = s.value("port", 1883).toInt();
            obj["clientId"] = s.value("clientId", "mqtt_monitor").toString();
            obj["user"]     = s.value("user").toString();
            obj["password"] = s.value("password").toString();
            obj["tls"]      = s.value("tls", false).toBool();
            QJsonArray subs;
            for (const auto& t : s.value("subscriptions").toStringList())
                subs.append(t);
            obj["subscriptions"] = subs;
            applyJson(obj);
        }
    }

    // Auto-save on connect
    QObject::connect(connectBtn, &QPushButton::clicked, [&]() {
        if (!client || !client->is_connected()) {
            QSettings s;
            auto obj = collectJson();
            s.setValue("broker",        obj["broker"].toString());
            s.setValue("port",          obj["port"].toInt());
            s.setValue("clientId",      obj["clientId"].toString());
            s.setValue("user",          obj["user"].toString());
            s.setValue("password",      obj["password"].toString());
            s.setValue("tls",           obj["tls"].toBool());
            QStringList sl;
            for (const auto& v : obj["subscriptions"].toArray())
                sl << v.toString();
            s.setValue("subscriptions", sl);
        }
    });

    // ── File menu ─────────────────────────────────────────────────────────────
    QMenu* fileMenu = window.menuBar()->addMenu("File");

    QAction* saveAct = fileMenu->addAction("Save profile…");
    saveAct->setShortcut(QKeySequence::Save);
    QObject::connect(saveAct, &QAction::triggered, [&]() {
        QString path = QFileDialog::getSaveFileName(
            &window, "Save profile", QDir::homePath(), "JSON (*.json)");
        if (path.isEmpty()) return;
        QFile f(path);
        if (!f.open(QIODevice::WriteOnly)) {
            QMessageBox::warning(&window, "Error", "Cannot save file.");
            return;
        }
        f.write(QJsonDocument(collectJson()).toJson());
        window.statusBar()->showMessage(QString("Saved: %1").arg(path));
    });

    QAction* loadAct = fileMenu->addAction("Load profile…");
    loadAct->setShortcut(QKeySequence::Open);
    QObject::connect(loadAct, &QAction::triggered, [&]() {
        QString path = QFileDialog::getOpenFileName(
            &window, "Load profile", QDir::homePath(), "JSON (*.json)");
        if (path.isEmpty()) return;
        QFile f(path);
        if (!f.open(QIODevice::ReadOnly)) {
            QMessageBox::warning(&window, "Error", "Cannot open file.");
            return;
        }
        QJsonParseError err;
        auto doc = QJsonDocument::fromJson(f.readAll(), &err);
        if (err.error != QJsonParseError::NoError || !doc.isObject()) {
            QMessageBox::warning(&window, "Error", "Invalid JSON file.");
            return;
        }
        applyJson(doc.object());
        window.statusBar()->showMessage(QString("Loaded: %1").arg(path));
    });

    fileMenu->addSeparator();

    QAction* exportCsvAct = fileMenu->addAction("Export messages (CSV)…");
    QObject::connect(exportCsvAct, &QAction::triggered, [&]() {
        if (!cb) {
            window.statusBar()->showMessage("No data to export.");
            return;
        }
        QString path = QFileDialog::getSaveFileName(
            &window, "Export CSV", QDir::homePath(), "CSV (*.csv)");
        if (path.isEmpty()) return;
        QFile f(path);
        if (!f.open(QIODevice::WriteOnly | QIODevice::Text)) {
            QMessageBox::warning(&window, "Error", "Cannot save file.");
            return;
        }
        QTextStream out(&f);
        out << "timestamp,topic,retained,payload\n";
        for (auto& [topic, ts, payload, retained] : cb->allMessages()) {
            QString safe = payload;
            safe.replace("\"", "\"\"");
            out << QString("\"%1\",\"%2\",%3,\"%4\"\n")
                .arg(ts).arg(topic).arg(retained ? 1 : 0).arg(safe);
        }
        window.statusBar()->showMessage(QString("Exported: %1").arg(path));
    });

    QAction* exportJsonAct = fileMenu->addAction("Export messages (JSON)…");
    QObject::connect(exportJsonAct, &QAction::triggered, [&]() {
        if (!cb) {
            window.statusBar()->showMessage("No data to export.");
            return;
        }
        QString path = QFileDialog::getSaveFileName(
            &window, "Export JSON", QDir::homePath(), "JSON (*.json)");
        if (path.isEmpty()) return;
        QFile f(path);
        if (!f.open(QIODevice::WriteOnly)) {
            QMessageBox::warning(&window, "Error", "Cannot save file.");
            return;
        }
        QJsonArray arr;
        for (auto& [topic, ts, payload, retained] : cb->allMessages()) {
            arr.append(QJsonObject{
                {"timestamp", ts},
                {"topic",     topic},
                {"retained",  retained},
                {"payload",   payload}
            });
        }
        f.write(QJsonDocument(arr).toJson());
        window.statusBar()->showMessage(QString("Exported: %1").arg(path));
    });

    QMenu* helpMenu = window.menuBar()->addMenu("Help");
    QAction* aboutAct = helpMenu->addAction("About…");
    QObject::connect(aboutAct, &QAction::triggered, [&]() {
        QMessageBox about(&window);
        about.setWindowTitle("About MQTT Monitor");
        about.setIconPixmap(QApplication::style()
            ->standardIcon(QStyle::SP_ComputerIcon)
            .pixmap(48, 48));
        about.setText("<b>MQTT Monitor</b>");
        about.setInformativeText(
            "Version 1.0<br>"
            "A tool for monitoring and debugging MQTT messages.<br><br>"
            "Features:<br>"
            "• Topic tree with per-topic message history<br>"
            "• Payload view: Raw, JSON, Hex, Base64<br>"
            "• Publish with QoS and Retained flag<br>"
            "• TLS/SSL support<br>"
            "• Export messages to CSV and JSON<br><br>"
            "Built with Qt6 and Paho MQTT C++.<br>"
            "<a href='https://github.com/mcrTechLab'>github.com/mcrTechLab</a>"
        );
        about.setTextFormat(Qt::RichText);
        about.exec();
    });

    window.show();
    return app.exec();
}
