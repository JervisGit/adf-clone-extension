# ADF Pipeline Clone

A beautiful Visual Studio Code extension that provides a visual pipeline editor for Azure Data Factory and Synapse Analytics, built with pure Canvas API and minimal dependencies.

## ✨ Features

- **Visual Pipeline Designer**: Drag-and-drop canvas interface for building data pipelines
- **Dedicated Activity Bar Tab**: Extension now has its own tab in the VS Code left navigation bar for easy access
- **Collapsible Activity Categories**: Organized activities in expandable/collapsible categories:
  - **Move & transform**: Copy data, Data Flow, Azure Function
  - **Synapse**: Notebook, Spark job definition, SQL script
  - **General**: Web, Get Metadata, Lookup, Delete, Wait, Validation, Script
  - **Iteration & conditionals**: ForEach, If Condition, Switch, Until, Set Variable, Append Variable, Filter
- **Activity Library**: Activities stored in a configuration file for easy maintenance
- **Connection Management**: Visual connections between activities with smooth bezier curves
- **Properties Panel**: Edit activity properties in real-time
- **Interactive Canvas**:
  - Drag activities around the canvas
  - Create connections by Shift+Click on activities
  - Right-click context menu for quick actions
  - Zoom in/out and fit to screen
  - Grid background for alignment
- **Beautiful UI**: Designed to match VS Code's theme with smooth animations
- **Zero External UI Dependencies**: Built with vanilla TypeScript and Canvas API

## 🚀 Getting Started

1. Install the extension
2. Open the Command Palette (`Ctrl+Shift+P` or `Cmd+Shift+P`)
3. Type "ADF: Open Pipeline Editor" and press Enter
4. Start building your pipeline!

## 🎯 Usage

### Adding Activities
- **Drag and Drop**: Drag any activity from the left sidebar onto the canvas
- **Command Palette**: Use "ADF: Add Activity" command

### Creating Connections
1. Hold `Shift` and click on the source activity
2. Move your mouse to the target activity
3. Click on the target activity to create the connection

### Editing Properties
- Click on any activity to select it
- Edit properties in the right panel
- Changes are reflected immediately on the canvas

### Canvas Controls
- **Drag**: Click and drag activities to reposition them
- **Select**: Click on an activity to select it
- **Delete**: Right-click on an activity and select "Delete"
- **Save**: Click the "💾 Save" button in the toolbar
- **Clear**: Click "🗑️ Clear" to remove all activities
- **Zoom**: Use the zoom buttons to adjust canvas scale

## 🛠️ Development

To run the extension in development mode:

```bash
# Install dependencies
npm install

# Compile the extension
npm run compile

# Run in watch mode
npm run watch
```

Press `F5` to open a new VS Code window with the extension loaded.

## 📋 Requirements

- Visual Studio Code 1.108.1 or higher
- No external dependencies required

## 🎨 Extension Commands

This extension contributes the following commands:

- `ADF: Open Pipeline Editor` - Opens the visual pipeline editor
- `ADF: Add Activity` - Adds an activity to the current pipeline

## 🐛 Known Issues

- Zoom functionality needs refinement for better UX
- Copy/Paste in context menu not yet implemented
- No persistence layer (pipelines reset on editor close)

## 📝 Release Notes

### 0.0.1

Initial release:
- Visual pipeline editor with drag-and-drop functionality
- Multiple activity types organized by category
- Connection management with visual arrows
- Properties panel for editing activity details
- Canvas controls (zoom, fit to screen, clear)
- Context menu for quick actions
- Beautiful UI matching VS Code theme

## 🤝 Contributing

Contributions are welcome! This extension is built with:
- TypeScript
- VS Code Extension API
- Canvas API (no React, no external UI libraries)

---

**Enjoy building beautiful data pipelines!** 🎉


---

## Following extension guidelines

Ensure that you've read through the extensions guidelines and follow the best practices for creating your extension.

* [Extension Guidelines](https://code.visualstudio.com/api/references/extension-guidelines)

## Working with Markdown

You can author your README using Visual Studio Code. Here are some useful editor keyboard shortcuts:

* Split the editor (`Cmd+\` on macOS or `Ctrl+\` on Windows and Linux).
* Toggle preview (`Shift+Cmd+V` on macOS or `Shift+Ctrl+V` on Windows and Linux).
* Press `Ctrl+Space` (Windows, Linux, macOS) to see a list of Markdown snippets.

## For more information

* [Visual Studio Code's Markdown Support](http://code.visualstudio.com/docs/languages/markdown)
* [Markdown Syntax Reference](https://help.github.com/articles/markdown-basics/)

**Enjoy!**
