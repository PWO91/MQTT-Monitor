#include <cstdlib>
#include <map>
#include <memory>

#include <QApplication>
#include <QGroupBox>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QLabel>
#include <QLineEdit>
#include <QMainWindow>
#include <QCheckBox>
#include <QClipboard>
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
#include <QStatusBar>
#include <QStringList>
#include <QTreeWidget>
#include <QTreeWidgetItem>
#include <QVBoxLayout>
#include <QWidget>

#include <mqtt/async_client.h>

static QString envOr(const char* name, const char* fallback) {
    const char* val = std::getenv(name);
    return val ? QString(val) : QString(fallback);
}

class TopicTree : public mqtt::callback {
    QTreeWidget* widget_;
    QTreeWidgetItem* root_ = nullptr;
    std::map<QString, QTreeWidgetItem*> nodes_;
    std::map<QString, QString> payloads_;
    QString brokerAddr_;

public:
    explicit TopicTree(QTreeWidget* w) : widget_(w) {}

    void reset(const QString& brokerAddr) {
        widget_->clear();
        nodes_.clear();
        payloads_.clear();
        brokerAddr_ = brokerAddr;
        root_ = new QTreeWidgetItem(widget_);
        root_->setText(0, brokerAddr);
        root_->setExpanded(false);
    }

    QString payloadFor(const QString& topic) const {
        auto it = payloads_.find(topic);
        return it != payloads_.end() ? it->second : QString();
    }

    void message_arrived(mqtt::const_message_ptr msg) override {
        QString topic   = QString::fromStdString(msg->get_topic());
        QString payload = QString::fromStdString(msg->to_string());

        QMetaObject::invokeMethod(widget_, [this, topic, payload]() {
            insert(topic, payload);
        }, Qt::QueuedConnection);
    }

private:
    void refreshLabel(QTreeWidgetItem* item, const QString& seg) {
        int n = item->childCount();
        item->setText(0, n > 0 ? QString("%1 (%2)").arg(seg).arg(n) : seg);
    }

    void insert(const QString& topic, const QString& payload) {
        if (!root_) return;

        payloads_[topic] = payload;

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
            parent->setText(1, payload.length() > 120
                ? payload.left(120) + "..."
                : payload);
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

// Rekonstruuje pełną ścieżkę tematu przez przejście w górę drzewa.
static QString itemTopic(QTreeWidgetItem* item) {
    QStringList parts;
    for (QTreeWidgetItem* cur = item; cur && cur->parent(); cur = cur->parent())
        parts.prepend(stripCount(cur->text(0)));
    return parts.join('/');
}

int main(int argc, char* argv[]) {
    QApplication app(argc, argv);
    app.setOrganizationName("mcrTechLab");
    app.setApplicationName("MQTTMonitor");

    QMainWindow window;
    window.setWindowTitle("MQTT Monitor");
    window.resize(750, 600);

    QWidget* central = new QWidget;
    QVBoxLayout* root = new QVBoxLayout(central);

    // Deklaracje wcześniej — lambdy subskrypcji muszą je widzieć
    std::unique_ptr<TopicTree> cb;
    std::shared_ptr<mqtt::async_client> client;

    // --- Górny panel z zakładkami ---
    QTabWidget* topTabs = new QTabWidget;
    topTabs->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Fixed);

    // Zakładka: Połączenie
    QWidget* connTab = new QWidget;
    QHBoxLayout* row = new QHBoxLayout(connTab);
    row->setContentsMargins(4, 4, 4, 4);

    QLineEdit* addrEdit = new QLineEdit(envOr("MQTT_BROKER", "localhost"));
    addrEdit->setPlaceholderText("Adres");
    addrEdit->setMinimumWidth(130);

    QSpinBox* portSpin = new QSpinBox;
    portSpin->setRange(1, 65535);
    portSpin->setValue(1883);
    portSpin->setFixedWidth(75);

    QLineEdit* userEdit = new QLineEdit(envOr("MQTT_USER", ""));
    userEdit->setPlaceholderText("Użytkownik");
    userEdit->setMinimumWidth(100);

    QLineEdit* passEdit = new QLineEdit(envOr("MQTT_PASSWORD", ""));
    passEdit->setPlaceholderText("Hasło");
    passEdit->setEchoMode(QLineEdit::Password);
    passEdit->setMinimumWidth(100);

    QPushButton* connectBtn = new QPushButton("Połącz");

    row->addWidget(new QLabel("Adres:"));
    row->addWidget(addrEdit);
    row->addWidget(new QLabel("Port:"));
    row->addWidget(portSpin);
    row->addWidget(new QLabel("User:"));
    row->addWidget(userEdit);
    row->addWidget(new QLabel("Hasło:"));
    row->addWidget(passEdit);
    row->addWidget(connectBtn);
    topTabs->addTab(connTab, "Połączenie");

    // Zakładka: Subskrypcje
    QWidget* subTab = new QWidget;
    QVBoxLayout* subLayout = new QVBoxLayout(subTab);
    subLayout->setContentsMargins(4, 4, 4, 4);

    QListWidget* subList = new QListWidget;
    subList->setFixedHeight(80);
    subList->addItem("#");
    subLayout->addWidget(subList);

    QHBoxLayout* subInputRow = new QHBoxLayout;
    QLineEdit* subEdit = new QLineEdit;
    subEdit->setPlaceholderText("Temat, np. sensors/# lub home/lamp");
    QPushButton* subAddBtn = new QPushButton("Dodaj");
    QPushButton* subRemBtn = new QPushButton("Usuń");
    subInputRow->addWidget(subEdit);
    subInputRow->addWidget(subAddBtn);
    subInputRow->addWidget(subRemBtn);
    subLayout->addLayout(subInputRow);
    topTabs->addTab(subTab, "Subskrypcje");

    QObject::connect(subAddBtn, &QPushButton::clicked, [&]() {
        QString t = subEdit->text().trimmed();
        if (t.isEmpty()) return;
        // Nie dodawaj duplikatów
        for (int i = 0; i < subList->count(); ++i)
            if (subList->item(i)->text() == t) return;
        subList->addItem(t);
        subEdit->clear();
        if (client && client->is_connected()) {
            try { client->subscribe(t.toStdString(), 0)->wait(); } catch (...) {}
            window.statusBar()->showMessage(QString("Zasubskrybowano: %1").arg(t));
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
        window.statusBar()->showMessage(QString("Odsubskrybowano: %1").arg(t));
    });

    root->addWidget(topTabs);

    // --- Splitter: drzewo | panel szczegółów ---
    QSplitter* splitter = new QSplitter(Qt::Vertical);

    QTreeWidget* tree = new QTreeWidget;
    tree->setColumnCount(2);
    tree->setHeaderLabels({"Temat", "Ostatnia wartość"});
    tree->header()->setStretchLastSection(true);
    tree->header()->resizeSection(0, 280);
    splitter->addWidget(tree);

    QTabWidget* tabs = new QTabWidget;

    QWidget* viewTab = new QWidget;
    QVBoxLayout* detailLayout = new QVBoxLayout(viewTab);
    detailLayout->setContentsMargins(4, 4, 4, 4);

    QHBoxLayout* detailHeader = new QHBoxLayout;
    QLabel* topicLabel = new QLabel("Kliknij temat aby zobaczyć szczegóły");
    topicLabel->setStyleSheet("font-weight: bold; color: gray;");
    QCheckBox* jsonCheck = new QCheckBox("Formatuj JSON");
    QPushButton* copyBtn = new QPushButton("Kopiuj");
    copyBtn->setFixedWidth(70);
    detailHeader->addWidget(topicLabel);
    detailHeader->addStretch();
    detailHeader->addWidget(jsonCheck);
    detailHeader->addWidget(copyBtn);
    detailLayout->addLayout(detailHeader);

    QPlainTextEdit* payloadView = new QPlainTextEdit;
    payloadView->setReadOnly(true);
    payloadView->setPlaceholderText("(brak danych)");
    payloadView->setFont(QFont("Menlo, Monaco, Courier New", 11));
    detailLayout->addWidget(payloadView);

    tabs->addTab(viewTab, "View");

    // --- Zakładka Publish ---
    QWidget* publishTab = new QWidget;
    QVBoxLayout* pubLayout = new QVBoxLayout(publishTab);
    pubLayout->setContentsMargins(4, 4, 4, 4);

    QHBoxLayout* pubTopicRow = new QHBoxLayout;
    pubTopicRow->addWidget(new QLabel("Temat:"));
    QLineEdit* pubTopicEdit = new QLineEdit;
    pubTopicEdit->setPlaceholderText("np. gateways/11330999/cmd");
    pubTopicRow->addWidget(pubTopicEdit);
    pubLayout->addLayout(pubTopicRow);

    QPlainTextEdit* pubMessageEdit = new QPlainTextEdit;
    pubMessageEdit->setPlaceholderText("Treść wiadomości...");
    pubMessageEdit->setFont(QFont("Menlo, Monaco, Courier New", 11));
    pubLayout->addWidget(pubMessageEdit);

    QPushButton* publishBtn = new QPushButton("Opublikuj");
    pubLayout->addWidget(publishBtn);

    tabs->addTab(publishTab, "Publish");
    splitter->addWidget(tabs);
    splitter->setStretchFactor(0, 3);
    splitter->setStretchFactor(1, 1);

    root->addWidget(splitter);

    window.setCentralWidget(central);
    window.statusBar()->showMessage("Gotowy");

    // --- Logika połączenia ---
    auto showPayload = [&](const QString& topic, const QString& raw) {
        topicLabel->setText(topic);
        topicLabel->setStyleSheet("font-weight: bold; color: black;");
        if (raw.isEmpty()) {
            payloadView->setPlainText("(węzeł bez wiadomości)");
            return;
        }
        if (jsonCheck->isChecked()) {
            QJsonParseError err;
            QJsonDocument doc = QJsonDocument::fromJson(raw.toUtf8(), &err);
            if (err.error == QJsonParseError::NoError) {
                payloadView->setPlainText(doc.toJson(QJsonDocument::Indented));
                return;
            }
        }
        payloadView->setPlainText(raw);
    };

    QObject::connect(copyBtn, &QPushButton::clicked, [&]() {
        QGuiApplication::clipboard()->setText(payloadView->toPlainText());
        window.statusBar()->showMessage("Skopiowano do schowka.");
    });

    // Zapamiętaj ostatnio wybrany temat żeby checkbox mógł odświeżyć widok
    QString currentTopic;

    QObject::connect(tree, &QTreeWidget::itemClicked,
        [&](QTreeWidgetItem* item, int) {
            if (!cb) return;
            currentTopic = itemTopic(item);
            if (currentTopic.isEmpty()) return;
            showPayload(currentTopic, cb->payloadFor(currentTopic));
            pubTopicEdit->setText(currentTopic);
        });

    QObject::connect(publishBtn, &QPushButton::clicked, [&]() {
        if (!client || !client->is_connected()) {
            window.statusBar()->showMessage("Nie połączono z brokerem.");
            return;
        }
        QString topic   = pubTopicEdit->text().trimmed();
        QString message = pubMessageEdit->toPlainText();
        if (topic.isEmpty()) {
            window.statusBar()->showMessage("Podaj temat przed publikacją.");
            return;
        }
        try {
            client->publish(topic.toStdString(), message.toStdString())->wait();
            window.statusBar()->showMessage(
                QString("Opublikowano na '%1'").arg(topic));
        } catch (const mqtt::exception& e) {
            window.statusBar()->showMessage(QString("Błąd publikacji: %1").arg(e.what()));
        }
    });

    QObject::connect(jsonCheck, &QCheckBox::toggled, [&]() {
        if (!cb || currentTopic.isEmpty()) return;
        showPayload(currentTopic, cb->payloadFor(currentTopic));
    });

    QObject::connect(connectBtn, &QPushButton::clicked, [&]() {
        if (client && client->is_connected()) {
            try { client->disconnect()->wait(); } catch (...) {}
            client.reset();
            cb.reset();
            connectBtn->setText("Połącz");
            window.statusBar()->showMessage("Rozłączono.");
            return;
        }

        QString addr = addrEdit->text().trimmed();
        int     port = portSpin->value();
        QString user = userEdit->text().trimmed();
        QString pass = passEdit->text();

        if (addr.isEmpty()) {
            window.statusBar()->showMessage("Podaj adres brokera.");
            return;
        }

        connectBtn->setEnabled(false);
        window.statusBar()->showMessage(
            QString("Łączenie z %1:%2...").arg(addr).arg(port));

        cb = std::make_unique<TopicTree>(tree);
        cb->reset(addr);

        QString uri = QString("tcp://%1:%2").arg(addr).arg(port);
        client = std::make_shared<mqtt::async_client>(uri.toStdString(), "qt_monitor");
        client->set_callback(*cb);

        mqtt::connect_options opts;
        opts.set_keep_alive_interval(20);
        opts.set_clean_session(true);
        if (!user.isEmpty()) {
            opts.set_user_name(user.toStdString());
            opts.set_password(pass.toStdString());
        }

        try {
            client->connect(opts)->wait();
            QStringList topics;
            for (int i = 0; i < subList->count(); ++i)
                topics << subList->item(i)->text();
            for (const QString& t : topics)
                client->subscribe(t.toStdString(), 0)->wait();
            connectBtn->setText("Rozłącz");
            window.statusBar()->showMessage(
                QString("Połączono z %1:%2 — %3 subskrypcji").arg(addr).arg(port).arg(topics.size()));
        } catch (const mqtt::exception& e) {
            window.statusBar()->showMessage(QString("Błąd: %1").arg(e.what()));
            client.reset();
            cb.reset();
        }

        connectBtn->setEnabled(true);
    });

    // --- Zapis/odczyt ustawień ---
    auto collectJson = [&]() -> QJsonObject {
        QJsonArray subs;
        for (int i = 0; i < subList->count(); ++i)
            subs.append(subList->item(i)->text());
        return QJsonObject{
            {"broker",        addrEdit->text()},
            {"port",          portSpin->value()},
            {"user",          userEdit->text()},
            {"password",      passEdit->text()},
            {"subscriptions", subs}
        };
    };

    auto applyJson = [&](const QJsonObject& obj) {
        addrEdit->setText(obj["broker"].toString());
        portSpin->setValue(obj["port"].toInt(1883));
        userEdit->setText(obj["user"].toString());
        passEdit->setText(obj["password"].toString());
        subList->clear();
        for (const auto& v : obj["subscriptions"].toArray())
            subList->addItem(v.toString());
        if (subList->count() == 0)
            subList->addItem("#");
    };

    // Auto-restore ostatniej sesji
    {
        QSettings s;
        if (s.contains("broker")) {
            QJsonObject obj;
            obj["broker"]   = s.value("broker").toString();
            obj["port"]     = s.value("port", 1883).toInt();
            obj["user"]     = s.value("user").toString();
            obj["password"] = s.value("password").toString();
            QJsonArray subs;
            for (const auto& t : s.value("subscriptions").toStringList())
                subs.append(t);
            obj["subscriptions"] = subs;
            applyJson(obj);
        }
    }

    // Auto-zapis przy połączeniu
    QObject::connect(connectBtn, &QPushButton::clicked, [&]() {
        if (!client || !client->is_connected()) {
            QSettings s;
            auto obj = collectJson();
            s.setValue("broker",        obj["broker"].toString());
            s.setValue("port",          obj["port"].toInt());
            s.setValue("user",          obj["user"].toString());
            s.setValue("password",      obj["password"].toString());
            QStringList sl;
            for (const auto& v : obj["subscriptions"].toArray())
                sl << v.toString();
            s.setValue("subscriptions", sl);
        }
    });

    // --- Menu Plik ---
    QMenu* fileMenu = window.menuBar()->addMenu("Plik");

    QAction* saveAct = fileMenu->addAction("Zapisz profil...");
    saveAct->setShortcut(QKeySequence::Save);
    QObject::connect(saveAct, &QAction::triggered, [&]() {
        QString path = QFileDialog::getSaveFileName(
            &window, "Zapisz profil", QDir::homePath(), "JSON (*.json)");
        if (path.isEmpty()) return;
        QFile f(path);
        if (!f.open(QIODevice::WriteOnly)) {
            QMessageBox::warning(&window, "Błąd", "Nie można zapisać pliku.");
            return;
        }
        f.write(QJsonDocument(collectJson()).toJson());
        window.statusBar()->showMessage(QString("Zapisano: %1").arg(path));
    });

    QAction* loadAct = fileMenu->addAction("Wczytaj profil...");
    loadAct->setShortcut(QKeySequence::Open);
    QObject::connect(loadAct, &QAction::triggered, [&]() {
        QString path = QFileDialog::getOpenFileName(
            &window, "Wczytaj profil", QDir::homePath(), "JSON (*.json)");
        if (path.isEmpty()) return;
        QFile f(path);
        if (!f.open(QIODevice::ReadOnly)) {
            QMessageBox::warning(&window, "Błąd", "Nie można otworzyć pliku.");
            return;
        }
        QJsonParseError err;
        auto doc = QJsonDocument::fromJson(f.readAll(), &err);
        if (err.error != QJsonParseError::NoError || !doc.isObject()) {
            QMessageBox::warning(&window, "Błąd", "Nieprawidłowy plik JSON.");
            return;
        }
        applyJson(doc.object());
        window.statusBar()->showMessage(QString("Wczytano: %1").arg(path));
    });

    window.show();
    return app.exec();
}
