# Замените функцию send_notification на эту версию с правильной логикой:

async def send_notification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправить уведомление ответственным лицам"""
    user_id = str(update.effective_user.id)
    user_data = user_states.get(user_id, {})
    
    # Получаем данные отправителя
    sender_info = get_user_permissions(user_id)
    
    # Получаем данные уведомления
    tp_data = user_data.get('tp_data', {})
    selected_tp = user_data.get('selected_tp')
    selected_vl = user_data.get('selected_vl')
    location = user_data.get('location', {})
    photo_id = user_data.get('photo_id')
    comment = user_data.get('comment', '')
    
    # Получаем данные из справочника (колонки A и B)
    branch_from_reference = tp_data.get('Филиал', '').strip()  # Колонка A
    res_from_reference = tp_data.get('РЭС', '').strip()  # Колонка B
    
    branch = user_data.get('branch')
    network = user_data.get('network')
    
    # Показываем анимированное сообщение отправки
    sending_messages = [
        "📨 Подготовка уведомления...",
        "🔍 Поиск ответственных лиц...",
        "📤 Отправка уведомлений...",
        "✅ Почти готово..."
    ]
    
    loading_msg = await update.message.reply_text(sending_messages[0])
    
    for msg_text in sending_messages[1:]:
        await asyncio.sleep(0.5)
        try:
            await loading_msg.edit_text(msg_text)
        except Exception:
            pass
    
    # Ищем всех ответственных в базе
    responsible_users = []
    
    logger.info(f"Ищем ответственных для:")
    logger.info(f"  Филиал из справочника: '{branch_from_reference}'")
    logger.info(f"  РЭС из справочника: '{res_from_reference}'")
    
    # Проходим по всем пользователям и проверяем колонку "Ответственный"
    for uid, udata in users_cache.items():
        responsible_for = udata.get('responsible', '').strip()
        
        if not responsible_for:
            continue
            
        # Проверяем совпадение с филиалом или РЭС из справочника
        if responsible_for == branch_from_reference or responsible_for == res_from_reference:
            responsible_users.append({
                'id': uid,
                'name': udata.get('name', 'Неизвестный'),
                'email': udata.get('email', ''),
                'responsible_for': responsible_for
            })
            logger.info(f"Найден ответственный: {udata.get('name')} (ID: {uid}) - отвечает за '{responsible_for}'")
    
    # Формируем текст уведомления
    notification_text = f"""🚨 НОВОЕ УВЕДОМЛЕНИЕ О БЕЗДОГОВОРНОМ ВОЛС

📍 Филиал: {branch}
📍 РЭС: {res_from_reference}
📍 ТП: {selected_tp}
⚡ ВЛ: {selected_vl}

👤 Отправитель: {sender_info['name']}
🕐 Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}"""

    if location:
        lat = location.get('latitude')
        lon = location.get('longitude')
        notification_text += f"\n📍 Координаты: {lat:.6f}, {lon:.6f}"
        notification_text += f"\n🗺 [Открыть на карте](https://maps.google.com/?q={lat},{lon})"
    
    if comment:
        notification_text += f"\n\n💬 Комментарий: {comment}"
    
    # Формируем список получателей для записи в хранилище
    recipients_info = ", ".join([f"{u['name']} ({u['id']})" for u in responsible_users]) if responsible_users else "Не найдены"
    
    # Сохраняем уведомление в хранилище
    notification_data = {
        'branch': branch,
        'res': res_from_reference,
        'tp': selected_tp,
        'vl': selected_vl,
        'sender_name': sender_info['name'],
        'sender_id': user_id,
        'recipient_name': recipients_info,
        'recipient_id': ", ".join([u['id'] for u in responsible_users]) if responsible_users else 'Не найдены',
        'datetime': datetime.now().strftime('%d.%m.%Y %H:%M'),
        'coordinates': f"{location.get('latitude', 0):.6f}, {location.get('longitude', 0):.6f}" if location else 'Не указаны',
        'comment': comment,
        'has_photo': bool(photo_id)
    }
    
    notifications_storage[network].append(notification_data)
    
    # Обновляем активность пользователя
    if user_id not in user_activity:
        user_activity[user_id] = {'last_activity': datetime.now(), 'count': 0}
    user_activity[user_id]['count'] += 1
    user_activity[user_id]['last_activity'] = datetime.now()
    
    # Отправляем уведомления всем найденным ответственным
    success_count = 0
    failed_users = []
    
    for responsible in responsible_users:
        try:
            # Отправляем текст
            await context.bot.send_message(
                chat_id=responsible['id'],
                text=notification_text,
                parse_mode='Markdown'
            )
            
            # Отправляем локацию
            if location:
                await context.bot.send_location(
                    chat_id=responsible['id'],
                    latitude=location.get('latitude'),
                    longitude=location.get('longitude')
                )
            
            # Отправляем фото
            if photo_id:
                await context.bot.send_photo(
                    chat_id=responsible['id'],
                    photo=photo_id,
                    caption=f"Фото с {selected_tp}"
                )
            
            success_count += 1
            
            # Отправляем email если включено
            if (responsible['email'] and 
                user_email_settings.get(responsible['id'], {}).get('enabled', True)):
                
                email_subject = f"ВОЛС: Уведомление от {sender_info['name']}"
                email_body = f"""Добрый день, {responsible['name']}!

Получено новое уведомление о бездоговорном ВОЛС.

{notification_text.replace('🚨', '').replace('📍', '•').replace('⚡', '•').replace('👤', '•').replace('🕐', '•').replace('💬', '•').replace('🗺', '')}

Для просмотра деталей откройте Telegram.

С уважением,
Бот ВОЛС Ассистент"""
                
                await send_email(responsible['email'], email_subject, email_body)
                
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления пользователю {responsible['name']} ({responsible['id']}): {e}")
            failed_users.append(f"{responsible['name']} ({responsible['id']}): {str(e)}")
    
    # Удаляем анимированное сообщение
    await loading_msg.delete()
    
    # Формируем результат
    if responsible_users:
        if success_count == len(responsible_users):
            result_text = f"""✅ Уведомления успешно отправлены!

📨 Получатели ({success_count}):"""
            for responsible in responsible_users:
                result_text += f"\n• {responsible['name']} (отвечает за {responsible['responsible_for']})"
                if responsible['email'] and user_email_settings.get(responsible['id'], {}).get('enabled', True):
                    result_text += f" 📧"
        else:
            result_text = f"""⚠️ Уведомления отправлены частично

✅ Успешно: {success_count} из {len(responsible_users)}

❌ Ошибки:"""
            for failed in failed_users:
                result_text += f"\n• {failed}"
    else:
        result_text = f"""❌ Ответственные не найдены

Для данной ТП не назначены ответственные лица.
Уведомление сохранено в системе и будет доступно в отчетах.

Отладочная информация:
- Филиал из справочника: "{branch_from_reference}"
- РЭС из справочника: "{res_from_reference}"
- Всего пользователей в базе: {len(users_cache)}

Список значений в колонке "Ответственный":"""
        # Показываем уникальные значения из колонки Ответственный для отладки
        unique_responsible = set(u.get('responsible', '') for u in users_cache.values() if u.get('responsible'))
        for resp in list(unique_responsible)[:10]:
            result_text += f"\n• {resp}"
        if len(unique_responsible) > 10:
            result_text += f"\n... и еще {len(unique_responsible) - 10}"
    
    # Очищаем состояние
    user_states[user_id] = {'state': f'branch_{branch}', 'branch': branch, 'network': network}
    
    await update.message.reply_text(
        result_text,
        reply_markup=get_branch_menu_keyboard()
    )
