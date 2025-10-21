
import logging
import os
import tempfile
import typing as tp

from plotly import express as px
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ContextTypes

from border_equeue_stats import constants as ct
from border_equeue_stats.analyze_equeue import (
    get_figure_waiting_hours_by_load, 
    get_figure_waiting_hours_by_reg,
    get_figure_vehicle_counts,
    get_figure_vehicle_count_per_regions
)

logger = logging.getLogger(ct.STAT_INTERFACE_LOGGER_NAME)


async def _send_chart_image(update: Update, context: ContextTypes.DEFAULT_TYPE, fig, caption: str = ""):
    """Helper function to send a plotly figure as an image to Telegram"""
    # Create temporary file for the image
    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
        fig.write_image(tmp_file.name, scale=2)
        
        with open(tmp_file.name, 'rb') as image_file:
            if update.message:
                await update.message.reply_photo(photo=image_file, caption=caption)
            elif update.callback_query:
                await update.callback_query.message.reply_photo(photo=image_file, caption=caption)
        
        # Clean up temporary file
        os.unlink(tmp_file.name)


async def plot_waiting_time_by_load(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                   queues_names: tp.List[str] = None,
                                   floor_value: tp.Optional[str] = None,
                                   aggregation_method: str = 'mean',
                                   time_range: tp.Optional[tp.Any] = None):
    """Generate and send waiting time chart by load time"""
    logger.info(f"user_id={update.effective_user.id}, generating waiting time by load chart")
    
    if queues_names is None:
        queues_names = [ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY]
    
    # Send "generating" message
    if update.message:
        generating_msg = await update.message.reply_text('📊 Generating waiting time chart...')
    else:
        generating_msg = await update.callback_query.message.reply_text('📊 Generating waiting time chart...')
    
    try:
        fig = get_figure_waiting_hours_by_load(queues_names=queues_names, floor_value=floor_value, 
                                        aggregation_method=aggregation_method, time_range=time_range)
        
        # Create caption with aggregation info
        aggregation_text = ""
        if floor_value == '5min':
            aggregation_text = f" (5-min {aggregation_method})"
        elif floor_value == 'h':
            aggregation_text = f" (hourly {aggregation_method})"
        elif floor_value == 'd':
            aggregation_text = f" (daily {aggregation_method})"
        elif floor_value == 'M':
            aggregation_text = f" (monthly {aggregation_method})"
        
        time_range_text = ""
        if time_range:
            days = time_range.days
            if days <= 7:
                time_range_text = f" - Last {days} day{'s' if days > 1 else ''}"
            elif days <= 31:
                weeks = days // 7
                time_range_text = f" - Last {weeks} week{'s' if weeks > 1 else ''}"
            elif days <= 365:
                months = days // 30
                time_range_text = f" - Last {months} month{'s' if months > 1 else ''}"
            else:
                years = days // 365
                time_range_text = f" - Last {years} year{'s' if years > 1 else ''}"
        
        caption = f"⏱️ Waiting Time by Load Date{aggregation_text}{time_range_text}\nQueues: {', '.join(queues_names)}"
        
        await _send_chart_image(update, context, fig, caption)
        
        # Delete generating message
        await generating_msg.delete()
        
    except Exception as e:
        logger.error(f"Error generating waiting time chart: {e}")
        await generating_msg.edit_text("❌ Error generating chart. Please try again later.")


async def plot_waiting_time_by_reg(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                  queues_names: tp.List[str] = None,
                                  floor_value: tp.Optional[str] = None,
                                  aggregation_method: str = 'mean',
                                  time_range: tp.Optional[tp.Any] = None):
    """Generate and send waiting time chart by registration time"""
    logger.info(f"user_id={update.effective_user.id}, generating waiting time by registration chart")
    
    if queues_names is None:
        queues_names = [ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY]
    
    # Send "generating" message
    if update.message:
        generating_msg = await update.message.reply_text('📊 Generating waiting time chart...')
    else:
        generating_msg = await update.callback_query.message.reply_text('📊 Generating waiting time chart...')
    
    try:
        fig = get_figure_waiting_hours_by_reg(queues_names=queues_names, floor_value=floor_value,
                                       aggregation_method=aggregation_method, time_range=time_range)
        
        # Create caption with aggregation info
        aggregation_text = ""
        if floor_value == '5min':
            aggregation_text = f" (5-min {aggregation_method})"
        elif floor_value == 'h':
            aggregation_text = f" (hourly {aggregation_method})"
        elif floor_value == 'd':
            aggregation_text = f" (daily {aggregation_method})"
        elif floor_value == 'M':
            aggregation_text = f" (monthly {aggregation_method})"
        
        time_range_text = ""
        if time_range:
            days = time_range.days
            if days <= 7:
                time_range_text = f" - Last {days} day{'s' if days > 1 else ''}"
            elif days <= 31:
                weeks = days // 7
                time_range_text = f" - Last {weeks} week{'s' if weeks > 1 else ''}"
            elif days <= 365:
                months = days // 30
                time_range_text = f" - Last {months} month{'s' if months > 1 else ''}"
            else:
                years = days // 365
                time_range_text = f" - Last {years} year{'s' if years > 1 else ''}"
        
        caption = f"⏱️ Waiting Time by Registration Date{aggregation_text}{time_range_text}\nQueues: {', '.join(queues_names)}"
        
        await _send_chart_image(update, context, fig, caption)
        
        # Delete generating message
        await generating_msg.delete()
        
    except Exception as e:
        logger.error(f"Error generating waiting time chart: {e}")
        await generating_msg.edit_text("❌ Error generating chart. Please try again later.")


async def plot_vehicle_count_chart(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                  queues_names: tp.List[str] = None,
                                  floor_value: tp.Optional[str] = None,
                                  aggregation_method: str = 'max',
                                  time_range: tp.Optional[tp.Any] = None):
    """Generate and send vehicle count chart"""
    logger.info(f"user_id={update.effective_user.id}, generating vehicle count chart")
    
    if queues_names is None:
        queues_names = [ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY]
    
    # Send "generating" message
    if update.message:
        generating_msg = await update.message.reply_text('📊 Generating vehicle count chart...')
    else:
        generating_msg = await update.callback_query.message.reply_text('📊 Generating vehicle count chart...')
    
    try:
        fig = get_figure_vehicle_counts(queues_names=queues_names, floor_value=floor_value,
                                 aggregation_method=aggregation_method, time_range=time_range)
        
        # Create caption with aggregation info
        aggregation_text = ""
        if floor_value == '5min':
            aggregation_text = f" (5-min {aggregation_method})"
        elif floor_value == 'h':
            aggregation_text = f" (hourly {aggregation_method})"
        elif floor_value == 'd':
            aggregation_text = f" (daily {aggregation_method})"
        elif floor_value == 'M':
            aggregation_text = f" (monthly {aggregation_method})"
        
        time_range_text = ""
        if time_range:
            days = time_range.days
            if days <= 7:
                time_range_text = f" - Last {days} day{'s' if days > 1 else ''}"
            elif days <= 31:
                weeks = days // 7
                time_range_text = f" - Last {weeks} week{'s' if weeks > 1 else ''}"
            elif days <= 365:
                months = days // 30
                time_range_text = f" - Last {months} month{'s' if months > 1 else ''}"
            else:
                years = days // 365
                time_range_text = f" - Last {years} year{'s' if years > 1 else ''}"
        
        caption = f"🚗 Vehicle Count Over Time{aggregation_text}{time_range_text}\nQueues: {', '.join(queues_names)}"
        
        await _send_chart_image(update, context, fig, caption)
        
        # Delete generating message
        await generating_msg.delete()
        
    except Exception as e:
        logger.error(f"Error generating vehicle count chart: {e}")
        await generating_msg.edit_text("❌ Error generating chart. Please try again later.")


async def plot_regional_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                queue_name: str = ct.CAR_LIVE_QUEUE_KEY,
                                plot_type: str = 'bar',
                                floor_value: tp.Optional[str] = None,
                                aggregation_method: str = 'sum',
                                time_range: tp.Optional[tp.Any] = None):
    """Generate and send regional analysis chart"""
    logger.info(f"user_id={update.effective_user.id}, generating regional analysis chart")
    
    # Send "generating" message
    if update.message:
        generating_msg = await update.message.reply_text('📊 Generating regional analysis chart...')
    else:
        generating_msg = await update.callback_query.message.reply_text('📊 Generating regional analysis chart...')
    
    try:
        fig = get_figure_vehicle_count_per_regions(queue_name=queue_name, plot_type=plot_type, floor_value=floor_value,
                                            aggregation_method=aggregation_method, time_range=time_range)
        
        # Create caption with aggregation info
        aggregation_text = ""
        if floor_value == '5min':
            aggregation_text = f" (5-min {aggregation_method})"
        elif floor_value == 'h':
            aggregation_text = f" (hourly {aggregation_method})"
        elif floor_value == 'd':
            aggregation_text = f" (daily {aggregation_method})"
        elif floor_value == 'M':
            aggregation_text = f" (monthly {aggregation_method})"
        
        time_range_text = ""
        if time_range:
            days = time_range.days
            if days <= 7:
                time_range_text = f" - Last {days} day{'s' if days > 1 else ''}"
            elif days <= 31:
                weeks = days // 7
                time_range_text = f" - Last {weeks} week{'s' if weeks > 1 else ''}"
            elif days <= 365:
                months = days // 30
                time_range_text = f" - Last {months} month{'s' if months > 1 else ''}"
            else:
                years = days // 365
                time_range_text = f" - Last {years} year{'s' if years > 1 else ''}"
        
        plot_type_emoji = "📊" if plot_type == 'bar' else "📈"
        caption = f"{plot_type_emoji} Regional Vehicle Count{aggregation_text}{time_range_text}\nQueue: {queue_name}\nChart type: {plot_type}"
        
        await _send_chart_image(update, context, fig, caption)
        
        # Delete generating message
        await generating_msg.delete()
        
    except Exception as e:
        logger.error(f"Error generating regional analysis chart: {e}")
        await generating_msg.edit_text("❌ Error generating chart. Please try again later.")


# Keep the original plot_queue function for backward compatibility
async def plot_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Original plot_queue function - now uses the new implementation"""
    logger.info(f"user_id={update.effective_user.id}, chat_id={update.effective_chat.id}, msg={update.message.text}")
    await plot_waiting_time_by_load(update, context)


# Conversation Handler States
SELECTING_CHART_TYPE, SELECTING_VEHICLES, SELECTING_QUEUES, SELECTING_AGGREGATION, SELECTING_TIME_RANGE, SELECTING_AGG_METHOD, GENERATING_CHART = range(7)

# Queue and Vehicle mappings
VEHICLE_TYPES = {
    "🚗 Car": "car",
    "🚌 Bus": "bus", 
    "🚛 Truck": "truck",
    "🏍️ Motorcycle": "motorcycle"
}

QUEUE_TYPES = {
    "🔴 Live Queue": "live",
    "⭐ Priority Queue": "priority",
    "🏛️ GPK Queue": "gpk"
}

AGGREGATION_OPTIONS = {
    "📍 All Points": None,
    "⏱️ 5-Minute": "5min",
    "⏰ Hourly": "h",
    "📅 Daily": "d",
    "📆 Monthly": "M"
}

AGGREGATION_METHODS = {
    "📊 Mean (Average)": "mean",
    "📈 Maximum": "max", 
    "📉 Minimum": "min",
    "🎯 Drop Points": "drop"
}


def _get_queue_key(vehicle_type: str, queue_type: str) -> str:
    """Convert vehicle and queue type to queue key"""
    mapping = {
        ("car", "live"): ct.CAR_LIVE_QUEUE_KEY,
        ("car", "priority"): ct.CAR_PRIORITY_KEY,
        ("bus", "live"): ct.BUS_LIVE_QUEUE_KEY,
        ("bus", "priority"): ct.BUS_PRIORITY_KEY,
        ("truck", "live"): ct.TRUCK_LIVE_QUEUE_KEY,
        ("truck", "priority"): ct.TRUCK_PRIORITY_KEY,
        ("truck", "gpk"): ct.TRUCK_GPK_KEY,
        ("motorcycle", "live"): ct.MOTORCYCLE_LIVE_QUEUE_KEY,
        ("motorcycle", "priority"): ct.MOTORCYCLE_PRIORITY_KEY,
    }
    return mapping.get((vehicle_type, queue_type))


async def start_chart_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the chart selection conversation"""
    logger.info(f"user_id={update.effective_user.id}, starting chart conversation")
    
    # Initialize user data
    context.user_data.clear()
    context.user_data['selected_vehicles'] = []
    context.user_data['selected_queues'] = []
    
    text = """📊 **Queue Statistics Chart Generator**

Let's create a custom chart! First, choose the type of chart you want:"""
    
    keyboard = [
        ["⏱️ Waiting Time by Load Date", "⏱️ Waiting Time by Registration"],
        ["🚗 Vehicle Count Over Time", "🗺️ Regional Analysis"],
        ["❌ Cancel"]
    ]
    
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text(text, reply_markup=reply_markup)
    
    return SELECTING_CHART_TYPE


async def select_chart_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle chart type selection"""
    chart_type = update.message.text
    
    if chart_type == "❌ Cancel":
        await update.message.reply_text("Chart generation cancelled.", reply_markup=ReplyKeyboardRemove())
        return -1  # End conversation
    
    # Store chart type
    context.user_data['chart_type'] = chart_type
    
    # For regional analysis, we need only one queue, so skip to queue selection
    if chart_type == "🗺️ Regional Analysis":
        text = """🗺️ **Regional Analysis**

This chart shows vehicle distribution by regions. Select ONE vehicle type:"""
        
        keyboard = [list(VEHICLE_TYPES.keys())[:2], list(VEHICLE_TYPES.keys())[2:], ["❌ Cancel"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text(text, reply_markup=reply_markup)
        
        context.user_data['regional_mode'] = True
        return SELECTING_VEHICLES
    
    # For other charts, continue with vehicle selection
    text = f"""✅ Selected: **{chart_type}**

Now select vehicle types. You can choose multiple vehicles by sending them one by one.
When you're done, type "✅ Done":"""
    
    keyboard = [list(VEHICLE_TYPES.keys())[:2], list(VEHICLE_TYPES.keys())[2:], ["✅ Done", "❌ Cancel"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=False, resize_keyboard=True)
    await update.message.reply_text(text, reply_markup=reply_markup)
    
    return SELECTING_VEHICLES


async def select_vehicles(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle vehicle type selection"""
    vehicle_input = update.message.text
    
    if vehicle_input == "❌ Cancel":
        await update.message.reply_text("Chart generation cancelled.", reply_markup=ReplyKeyboardRemove())
        return -1
    
    if vehicle_input == "✅ Done":
        if not context.user_data['selected_vehicles']:
            await update.message.reply_text("⚠️ Please select at least one vehicle type first!")
            return SELECTING_VEHICLES
        
        # Move to queue selection
        selected_vehicles_text = ", ".join(context.user_data['selected_vehicles'])
        text = f"""✅ Selected vehicles: **{selected_vehicles_text}**

Now select queue types. You can choose multiple queues by sending them one by one.
When you're done, type "✅ Done":"""
        
        keyboard = [list(QUEUE_TYPES.keys()), ["✅ Done", "❌ Cancel"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=False, resize_keyboard=True)
        await update.message.reply_text(text, reply_markup=reply_markup)
        
        return SELECTING_QUEUES
    
    # Handle vehicle selection
    if vehicle_input in VEHICLE_TYPES:
        vehicle_type = VEHICLE_TYPES[vehicle_input]
        
        if vehicle_type not in context.user_data['selected_vehicles']:
            context.user_data['selected_vehicles'].append(vehicle_type)
            
            # For regional analysis, automatically proceed to queue selection
            if context.user_data.get('regional_mode'):
                context.user_data['selected_vehicles'] = [vehicle_type]  # Only one vehicle for regional
                
                text = f"""✅ Selected: **{vehicle_input}**

Now select ONE queue type for regional analysis:"""
                
                keyboard = [list(QUEUE_TYPES.keys()), ["❌ Cancel"]]
                reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
                await update.message.reply_text(text, reply_markup=reply_markup)
                
                return SELECTING_QUEUES
            
            await update.message.reply_text(f"✅ Added: **{vehicle_input}**\nCurrent selection: {', '.join(context.user_data['selected_vehicles'])}")
        else:
            await update.message.reply_text(f"⚠️ **{vehicle_input}** already selected!")
    else:
        await update.message.reply_text("⚠️ Please select a valid vehicle type from the keyboard.")
    
    return SELECTING_VEHICLES


async def select_queues(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle queue type selection"""
    queue_input = update.message.text
    
    if queue_input == "❌ Cancel":
        await update.message.reply_text("Chart generation cancelled.", reply_markup=ReplyKeyboardRemove())
        return -1
    
    if queue_input == "✅ Done":
        if not context.user_data['selected_queues']:
            await update.message.reply_text("⚠️ Please select at least one queue type first!")
            return SELECTING_QUEUES
        
        # Move to aggregation selection
        return await select_aggregation_step(update, context)
    
    # Handle queue selection
    if queue_input in QUEUE_TYPES:
        queue_type = QUEUE_TYPES[queue_input]
        
        if queue_type not in context.user_data['selected_queues']:
            context.user_data['selected_queues'].append(queue_type)
            
            # For regional analysis, automatically proceed to aggregation
            if context.user_data.get('regional_mode'):
                context.user_data['selected_queues'] = [queue_type]  # Only one queue for regional
                return await select_aggregation_step(update, context)
            
            await update.message.reply_text(f"✅ Added: **{queue_input}**\nCurrent selection: {', '.join(context.user_data['selected_queues'])}")
        else:
            await update.message.reply_text(f"⚠️ **{queue_input}** already selected!")
    else:
        await update.message.reply_text("⚠️ Please select a valid queue type from the keyboard.")
    
    return SELECTING_QUEUES


async def select_aggregation_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Move to aggregation selection step"""
    selected_vehicles_text = ", ".join(context.user_data['selected_vehicles'])
    selected_queues_text = ", ".join(context.user_data['selected_queues'])
    
    text = f"""✅ **Selection Summary:**
• Chart: {context.user_data['chart_type']}
• Vehicles: {selected_vehicles_text}
• Queues: {selected_queues_text}

Choose time aggregation (how to group data points):"""
    
    keyboard = [
        list(AGGREGATION_OPTIONS.keys())[:3],
        list(AGGREGATION_OPTIONS.keys())[3:],
        ["❌ Cancel"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text(text, reply_markup=reply_markup)
    
    return SELECTING_AGGREGATION


async def select_aggregation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle aggregation selection and move to time range selection"""
    aggregation_input = update.message.text
    
    if aggregation_input == "❌ Cancel":
        await update.message.reply_text("Chart generation cancelled.", reply_markup=ReplyKeyboardRemove())
        return -1
    
    if aggregation_input not in AGGREGATION_OPTIONS:
        await update.message.reply_text("⚠️ Please select a valid aggregation option from the keyboard.")
        return SELECTING_AGGREGATION
    
    # Store aggregation choice
    floor_value = AGGREGATION_OPTIONS[aggregation_input]
    context.user_data['aggregation'] = floor_value
    context.user_data['aggregation_display'] = aggregation_input
    
    # Move to time range selection
    return await select_time_range_step(update, context)


async def select_time_range_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Move to time range selection step with smart defaults based on aggregation"""
    from border_equeue_stats.data_storage.parquet_storage import get_recommended_time_ranges
    
    floor_value = context.user_data['aggregation']
    aggregation_display = context.user_data['aggregation_display']
    
    # Get recommended time ranges for this aggregation
    time_ranges = get_recommended_time_ranges(floor_value)
    
    text = f"""⏱️ **Time Range Selection**

Aggregation: {aggregation_display}

Choose the time period for your analysis:
(Recommendations are optimized for your aggregation choice)"""
    
    # Create keyboard with recommended ranges
    keyboard = []
    range_keys = list(time_ranges.keys())
    for i in range(0, len(range_keys), 2):
        row = range_keys[i:i+2]
        keyboard.append(row)
    keyboard.append(["❌ Cancel"])
    
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text(text, reply_markup=reply_markup)
    
    # Store available ranges for validation
    context.user_data['available_time_ranges'] = time_ranges
    
    return SELECTING_TIME_RANGE


async def select_time_range(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle time range selection and move to aggregation method selection"""
    time_range_input = update.message.text
    
    if time_range_input == "❌ Cancel":
        await update.message.reply_text("Chart generation cancelled.", reply_markup=ReplyKeyboardRemove())
        return -1
    
    available_ranges = context.user_data.get('available_time_ranges', {})
    if time_range_input not in available_ranges:
        await update.message.reply_text("⚠️ Please select a valid time range from the keyboard.")
        return SELECTING_TIME_RANGE
    
    # Store time range choice
    time_range = available_ranges[time_range_input]
    context.user_data['time_range'] = time_range
    context.user_data['time_range_display'] = time_range_input
    
    # Move to aggregation method selection (only if not "All Points")
    if context.user_data['aggregation'] is None:
        # Skip aggregation method for "All Points"
        context.user_data['aggregation_method'] = 'mean'  # Default, won't be used
        await update.message.reply_text("🎯 Generating your custom chart...", reply_markup=ReplyKeyboardRemove())
        await generate_selected_chart(update, context)
        return -1
    else:
        return await select_aggregation_method_step(update, context)


async def select_aggregation_method_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Move to aggregation method selection step"""
    chart_type = context.user_data['chart_type']
    aggregation_display = context.user_data['aggregation_display']
    time_range_display = context.user_data['time_range_display']
    
    # Customize method descriptions based on chart type
    if "Waiting Time" in chart_type:
        method_description = """
📊 **Mean**: Average waiting time in each period
📈 **Maximum**: Longest waiting time in each period  
📉 **Minimum**: Shortest waiting time in each period
🎯 **Drop Points**: Remove intermediate points (faster)"""
    elif "Vehicle Count" in chart_type:
        method_description = """
📊 **Mean**: Average vehicle count in each period
📈 **Maximum**: Peak vehicle count in each period
📉 **Minimum**: Lowest vehicle count in each period  
🎯 **Drop Points**: Remove intermediate points (faster)"""
    else:
        method_description = """
📊 **Mean**: Average values in each period
📈 **Maximum**: Peak values in each period
📉 **Minimum**: Lowest values in each period
🎯 **Drop Points**: Remove intermediate points (faster)"""
    
    text = f"""🔧 **Aggregation Method**

Chart: {chart_type}
Time Period: {aggregation_display}
Range: {time_range_display}

How should values be combined within each time period?
{method_description}"""
    
    keyboard = [
        list(AGGREGATION_METHODS.keys())[:2],
        list(AGGREGATION_METHODS.keys())[2:],
        ["❌ Cancel"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text(text, reply_markup=reply_markup)
    
    return SELECTING_AGG_METHOD


async def select_aggregation_method(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle aggregation method selection and generate chart"""
    method_input = update.message.text
    
    if method_input == "❌ Cancel":
        await update.message.reply_text("Chart generation cancelled.", reply_markup=ReplyKeyboardRemove())
        return -1
    
    if method_input not in AGGREGATION_METHODS:
        await update.message.reply_text("⚠️ Please select a valid aggregation method from the keyboard.")
        return SELECTING_AGG_METHOD
    
    # Store aggregation method choice
    aggregation_method = AGGREGATION_METHODS[method_input]
    context.user_data['aggregation_method'] = aggregation_method
    
    # Remove keyboard and generate chart
    await update.message.reply_text("🎯 Generating your custom chart...", reply_markup=ReplyKeyboardRemove())
    
    # Generate the chart
    await generate_selected_chart(update, context)
    
    return -1  # End conversation


async def generate_selected_chart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate the chart based on user selections"""
    chart_type = context.user_data['chart_type']
    selected_vehicles = context.user_data['selected_vehicles']
    selected_queues = context.user_data['selected_queues']
    floor_value = context.user_data['aggregation']
    time_range = context.user_data['time_range']
    aggregation_method = context.user_data['aggregation_method']
    
    # Build queue names list
    queue_names = []
    for vehicle in selected_vehicles:
        for queue in selected_queues:
            queue_key = _get_queue_key(vehicle, queue)
            if queue_key:
                queue_names.append(queue_key)
    
    try:
        if chart_type == "⏱️ Waiting Time by Load Date":
            await plot_waiting_time_by_load(update, context, queue_names, floor_value, aggregation_method, time_range)
        elif chart_type == "⏱️ Waiting Time by Registration":
            await plot_waiting_time_by_reg(update, context, queue_names, floor_value, aggregation_method, time_range)
        elif chart_type == "🚗 Vehicle Count Over Time":
            await plot_vehicle_count_chart(update, context, queue_names, floor_value, aggregation_method, time_range)
        elif chart_type == "🗺️ Regional Analysis":
            # For regional analysis, use only the first queue
            queue_name = queue_names[0] if queue_names else ct.CAR_LIVE_QUEUE_KEY
            await plot_regional_analysis(update, context, queue_name, 'bar', floor_value, aggregation_method, time_range)
        
        # Show completion message
        await update.message.reply_text("✅ Chart generated successfully! Use /chart to create another chart.")
        
    except Exception as e:
        logger.error(f"Error in generate_selected_chart: {e}")
        await update.message.reply_text("❌ Error generating chart. Please try again later.")


async def cancel_chart_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel the conversation"""
    await update.message.reply_text("Chart generation cancelled.", reply_markup=ReplyKeyboardRemove())
    return -1
