async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE, network: str, permissions: Dict):
    """Генерация отчета"""
    try:
        user_id = str(update.effective_user.id)
        notifications = notifications_storage[network]
        
        if not notifications:
            await update.message.reply_text("📊 Нет данных для отчета")
            return
        
        # Фильтруем уведомления в зависимости от прав
        if permissions['branch'] != 'All':
            notifications = [n for n in notifications if n['branch'] == permissions['branch']]
        
        if not notifications:
            await update.message.reply_text("📊 Нет данных для отчета по вашему филиалу")
            return
        
        # Создаем DataFrame
        df = pd.DataFrame(notifications)
        
        # Проверяем наличие необходимых колонок
        required_columns = ['branch', 'res', 'sender_name', 'sender_id', 'recipient_name', 'recipient_id', 'datetime', 'coordinates']
        existing_columns = [col for col in required_columns if col in df.columns]
        
        if not existing_columns:
            await update.message.reply_text("📊 Недостаточно данных для формирования отчета")
            return
            
        df = df[existing_columns]
        
        # Переименовываем колонки
        column_mapping = {
            'branch': 'ФИЛИАЛ',
            'res': 'РЭС', 
            'sender_name': 'ФИО ОТПРАВИТЕЛЯ',
            'sender_id': 'ID ОТПРАВИТЕЛЯ',
            'recipient_name': 'ФИО ПОЛУЧАТЕЛЯ',
            'recipient_id': 'ID ПОЛУЧАТЕЛЯ',
            'datetime': 'ВРЕМЯ ДАТА',
            'coordinates': 'КООРДИНАТЫ'
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Создаем Excel файл
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Уведомления', index=False)
            
            # Форматирование
            workbook = writer.book
            worksheet = writer.sheets['Уведомления']
            
            # Формат заголовков
            header_format = workbook.add_format({
                'bg_color': '#FFE6E6',
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'border': 1
            })
            
            # Применяем формат к заголовкам
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Автоподбор ширины колонок
            for i, col in enumerate(df.columns):
                column_len = df[col].astype(str).map(len).max()
                column_len = max(column_len, len(col)) + 2
                worksheet.set_column(i, i, column_len)
        
        # ВАЖНО: Перемещаем указатель в начало после записи
        output.seek(0)
        
        # Отправляем файл в чат
        network_name = "РОССЕТИ КУБАНЬ" if network == 'RK' else "РОССЕТИ ЮГ"
        filename = f"Уведомления_{network_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Создаем InputFile для правильной отправки
        from telegram import InputFile
        
        await update.message.reply_document(
            document=InputFile(output, filename=filename),
            caption=f"📊 Отчет по уведомлениям {network_name}"
        )
        
        # Сохраняем последний отчет пользователя
        output.seek(0)
        last_reports[user_id] = {
            'data': BytesIO(output.read()),
            'filename': filename,
            'type': f"Уведомления {network_name}",
            'datetime': datetime.now().strftime('%d.%m.%Y %H:%M')
        }
        
        # Отправляем на email если указан и включены уведомления
        user_data = users_cache.get(user_id, {})
        user_email = user_data.get('email', '')
        email_enabled = user_email_settings.get(user_id, {}).get('enabled', True)
        
        if user_email and email_enabled:
            output.seek(0)
            subject = f"Отчет по уведомлениям {network_name}"
            body = f"""Добрый день!

Направляем вам отчет по уведомлениям {network_name} от {datetime.now().strftime('%d.%m.%Y %H:%M')}.

Всего уведомлений в отчете: {len(df)}

С уважением,
Бот ВОЛС Ассистент"""
            
            if await send_email(user_email, subject, body, output, filename):
                await update.message.reply_text(f"📧 Отчет также отправлен на {user_email}")
            else:
                await update.message.reply_text("⚠️ Не удалось отправить отчет на email")
                
    except Exception as e:
        logger.error(f"Ошибка генерации отчета: {e}")
        await update.message.reply_text(f"❌ Ошибка генерации отчета: {str(e)}")

# Также исправим отправку последнего отчета на почту в handle_message
# Найдите блок с text == '📨 Отправить последний отчет на почту' и замените на:

        elif text == '📨 Отправить последний отчет на почту':
            user_data = users_cache.get(user_id, {})
            user_email = user_data.get('email', '')
            
            if not user_email:
                await update.message.reply_text("❌ У вас не указан email в системе")
                return
            
            # Ищем последний отчет пользователя
            last_report = last_reports.get(user_id)
            
            if last_report:
                report_data = last_report['data']
                report_name = last_report['filename']
                report_type = last_report['type']
                
                subject = f"Отчет: {report_name}"
                body = f"""Добрый день, {user_data.get('name', '')}!

По вашему запросу направляем последний сформированный отчет.

Тип отчета: {report_type}
Дата формирования: {last_report['datetime']}

С уважением,
Бот ВОЛС Ассистент"""
                
                # ВАЖНО: Сбрасываем позицию перед отправкой
                report_data.seek(0)
                
                if await send_email(user_email, subject, body, report_data, report_name):
                    await update.message.reply_text(f"✅ Отчет отправлен на {user_email}")
                else:
                    await update.message.reply_text("❌ Ошибка отправки отчета")
            else:
                await update.message.reply_text("❌ У вас пока нет сформированных отчетов")
